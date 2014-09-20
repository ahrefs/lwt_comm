open Lwt
module M = Lwt_mq_map

type -'a mq_sink =
| Si_Acks of ('a * unit Lwt.u) M.sink
| Si_No_acks of 'a M.sink

type +'a mq_source =
| So_Acks of ('a * unit Lwt.u) M.source
| So_No_acks of 'a M.source

type (-'snd, +'rcv, 'kind) conn =
  { snd_sink : 'snd mq_sink
  ; snd_closed : bool ref
  ; rcv_source : 'rcv mq_source
  ; rcv_closed : bool ref
  ; on_close : unit Lazy.t
      (* value is forced when connection is closed for sendind and for
         receiving.  When forced, it removes connection from server_ctl.conns.
       *)
  }

type conn_id = int

type server_state =
| Ss_running
| Ss_closing of (exn * unit Lwt_condition.t * unit Lwt.t ref)
| Ss_closed of exn

type server_ctl =
  { sc_shtd_waiter : unit Lwt.t
  ; sc_shtd : unit Lazy.t
  ; mutable sc_state : server_state
  ; conns : (conn_id, exn -> unit) Hashtbl.t
  ; mutable instances : int
  }

type (-'req, +'resp, 'kind) server =
  { shandler : ('resp, 'req, 'kind) conn -> unit Lwt.t
  ; sctl : server_ctl
  }

type 'snd confirmation = unit Lwt.u

let ack wak = wakeup wak ()

let nack wak exn = wakeup_exn wak exn

let send conn s =
  match conn.snd_sink with
  | Si_Acks si ->
      let (wai, wak) = wait () (* to think: cancellation *) in
      lwt () = M.sink_put si (s, wak) in
      wai
  | Si_No_acks si ->
      M.sink_put si s

let recv_ack conn =
  match conn.rcv_source with
  | So_Acks so -> begin
      try_lwt
        M.source_take so
      with
        Lwt_mq.Closed e -> fail e
    end
  | So_No_acks _ -> fail @@ Invalid_argument
      "Lwt_comm.recv_ack: connection doesn't need ACKs"

let recv conn =
  match conn.rcv_source with
  | So_Acks so -> begin
      try_lwt
        lwt (r, cnf) = M.source_take so in
        ack cnf;
        return r
      with
        Lwt_mq.Closed e -> fail e
    end
  | So_No_acks so -> begin
      try_lwt
        M.source_take so
      with
        Lwt_mq.Closed e -> fail e
    end

let recv_opt conn =
  try
    lwt r = recv conn in
    return @@ Some r
  with
    End_of_file -> return_none

let recv_res_ack conn =
  try_lwt
    lwt r = recv_ack conn in
    return @@ `Ok r
  with
    e -> return @@ `Error e

let recv_res conn =
  try_lwt
    lwt r = recv conn in
    return @@ `Ok r
  with
    e -> return @@ `Error e

let run_on_close conn =
  if !(conn.snd_closed) && !(conn.rcv_closed)
  then Lazy.force conn.on_close
  else ()

let shutdown_sd exn l =
  if !(l.snd_closed)
  then ()
  else begin
    l.snd_closed := true;
    begin
    (* try *)
      match l.snd_sink with
      | Si_Acks si -> M.close_sink si exn
      | Si_No_acks si -> M.close_sink si exn
    (*
    with e ->
      Printf.eprintf "close err: %s\n%!" (Printexc.to_string e);
      raise e
    *)
    end;
    run_on_close l
  end

let shutdown_rc exn l =
  if !(l.rcv_closed)
  then ()
  else begin
    l.rcv_closed := true;
    begin
    (* try *)
      match l.rcv_source with
      | So_Acks so -> M.close_source so exn
      | So_No_acks so -> M.close_source so exn
    (*
    with e ->
      Printf.eprintf "close err: %s\n%!" (Printexc.to_string e);
      raise e
    *)
    end;
    run_on_close l
  end

let shutdown ?(exn = End_of_file) conn cmd =
  match cmd with
  | Unix.SHUTDOWN_SEND ->
      shutdown_sd exn conn
  | Unix.SHUTDOWN_RECEIVE ->
      shutdown_rc exn conn
  | Unix.SHUTDOWN_ALL ->
      shutdown_sd exn conn;
      shutdown_rc exn conn

let close ?(exn = End_of_file) conn =
  shutdown ~exn conn Unix.SHUTDOWN_ALL

let on_shutdown_do_nothing () = return_unit

let duplex ?(on_shutdown = on_shutdown_do_nothing) serverfunc =
  let conns = Hashtbl.create 7 in
  let sctl =
    let (wt, wk) = Lwt.wait () in
    { sc_shtd_waiter = begin
        lwt () = wt in
        lwt () = on_shutdown () in
        return_unit
      end
    ; sc_shtd = lazy (Lwt.wakeup wk ())
    ; sc_state = Ss_running
    ; conns = conns
    ; instances = 0
    }
  in
  ( { shandler = serverfunc
    ; sctl = sctl
    }
  , sctl
  )

exception Server_shut_down

let timeout_waiter timeout =
  if timeout <= 0.
  then
    let (wai, _wak) = task () in
    wai
  else
    Lwt_unix.sleep timeout >|= fun () -> `Timeout

let shutdown_server_wait ?(exn = Server_shut_down) ?(timeout = 0.) sctl =
  let shutdown_return () =
    sctl.sc_state <- Ss_closed exn;
    Lazy.force sctl.sc_shtd;
    lwt () = sctl.sc_shtd_waiter in
    return `Shut_down
  in
  if sctl.instances = 0
  then
    shutdown_return ()
  else
    let (cond, close_waiter_ref) =
      match sctl.sc_state with
      | Ss_closed _exn -> assert false
          (* if server is closed, instances=0, this if-branch is unreachable. *)
      | Ss_running ->
          let cond = Lwt_condition.create () in
          let close_waiter_ref = ref (Lwt_condition.wait cond) in
          sctl.sc_state <- Ss_closing (exn, cond, close_waiter_ref);
          (cond, close_waiter_ref)
      | Ss_closing (_old_exn, cond, close_waiter_ref) ->
          sctl.sc_state <- Ss_closing (exn, cond, close_waiter_ref);
          (cond, close_waiter_ref)
    in
    let timeout_waiter = timeout_waiter timeout in
    let rec loop () =
      assert (sctl.instances > 0);
      Printf.eprintf "shutdown_server_wait loop, %i more instances\n%!"
        sctl.instances;
      let close_waiter = !close_waiter_ref >|= fun () -> `Conn_closed in
      lwt event = choose [timeout_waiter; close_waiter] in
      let insts = sctl.instances in
      match event with
      | `Timeout -> return @@ `Instances_exist insts
      | `Conn_closed ->
          if insts = 0
          then begin
            cancel timeout_waiter;
            shutdown_return ()
          end else begin
            close_waiter_ref := Lwt_condition.wait cond;
            loop ()
          end
    in
      loop ()

let shutdown_server ?(exn = Server_shut_down) sctl =
  Printf.eprintf
    "shutdown_server: conns: %i instances: %i\n%!"
    (Hashtbl.length sctl.conns) sctl.instances;
  Hashtbl.iter (fun _cid closefunc -> closefunc exn) sctl.conns;
  Hashtbl.clear sctl.conns;
  match_lwt shutdown_server_wait ~exn ~timeout:0. sctl with
  | `Instances_exist _ -> assert false  (* timeout is infinite *)
  | `Shut_down -> return_unit

let shutdown_server_wait_infinite ?(exn = Server_shut_down) sctl =
  match_lwt shutdown_server_wait ~timeout:0. ~exn sctl with
  | `Instances_exist _n -> assert false
  | `Shut_down -> return_unit

let wait_for_server_shutdown sctl =
  sctl.sc_shtd_waiter

let run_lwt_server server server_conn =
  let ctl = server.sctl in
  try_lwt
    ctl.instances <- ctl.instances + 1;
    lwt () = server.shandler server_conn in
    close server_conn;
    return_unit
  with exn ->
    close server_conn ~exn;
    return_unit
  finally
    ctl.instances <- ctl.instances - 1;
    begin match ctl.sc_state with
    | Ss_running | Ss_closed _ -> ()
    | Ss_closing (_exn, cond, _close_waiter) ->
        Lwt_condition.signal cond ()
    end;
    return_unit

let si_so_pair () =
  M.sink_source @@ M.of_mq @@ Lwt_mq.create ()

let si_so_link acks =
  if acks
  then
    let (si, so) = si_so_pair () in
    (Si_Acks si, So_Acks so)
  else
    let (si, so) = si_so_pair () in
    (Si_No_acks si, So_No_acks so)

let conn_pair ~ack_req ~ack_resp on_close =
  let (s2c_sink, s2c_source) = si_so_link ack_resp
  and (c2s_sink, c2s_source) = si_so_link ack_req in
  let s2c_closed = ref false
  and c2s_closed = ref false in
  let server_conn =
    { snd_sink = s2c_sink
    ; snd_closed = s2c_closed
    ; rcv_source = c2s_source
    ; rcv_closed = c2s_closed
    ; on_close = on_close
    }
  and client_conn =
    { snd_sink = c2s_sink
    ; snd_closed = c2s_closed
    ; rcv_source = s2c_source
    ; rcv_closed = s2c_closed
    ; on_close = on_close
    }
  in
  (server_conn, client_conn)

let map_sink m s =
  match s with
  | Si_No_acks si -> Si_No_acks (M.map_sink m si)
  | Si_Acks si -> Si_Acks (M.map_sink (fun (msg, cfm) -> (m msg, cfm)) si)

let map_source m s =
  match s with
  | So_No_acks so -> So_No_acks (M.map_source m so)
  | So_Acks so -> So_Acks (M.map_source (fun (msg, cfm) -> (m msg, cfm)) so)

let map_conn map_req map_resp conn =
  { snd_sink = map_sink map_req conn.snd_sink
  ; rcv_source = map_source map_resp conn.rcv_source
  ; snd_closed = conn.snd_closed
  ; rcv_closed = conn.rcv_closed
  ; on_close = conn.on_close
  }

let map_server map_req map_resp server =
  let old_shandler = server.shandler in
  { server with shandler = fun conn ->
      let conn' = map_conn map_resp map_req conn in
      old_shandler conn'
  }

let next_conn_id =
  let cur = ref 0 in
  fun () -> (incr cur; !cur)

let connect
 : ?ack_req:bool -> ?ack_resp:bool ->
   ('req, 'resp, [> `Connect] as 'k) server ->
   ('req, 'resp, 'k) conn
 = fun ?(ack_req = true) ?(ack_resp = true) server ->
  match server.sctl.sc_state with
  | Ss_running ->
      let conn_id = next_conn_id () in
      let on_conn_close = lazy (Hashtbl.remove server.sctl.conns conn_id) in
      let (server_conn, client_conn) =
        conn_pair ~ack_req ~ack_resp on_conn_close in
      let closefunc exn = close client_conn ~exn in
      Hashtbl.add server.sctl.conns conn_id closefunc;
      Lwt.ignore_result (run_lwt_server server server_conn);
      client_conn
  | Ss_closed exn | Ss_closing (exn, _, _) -> raise exn

let with_connection server func =
  let client_conn = connect server in
  try_lwt
    func client_conn
  finally
    close client_conn;
    return_unit

type (+'req, -'resp, 'k) unix_func =
  ('req, 'resp, 'k) conn -> Lwt_unix.file_descr -> unit Lwt.t

let run_unix_func func conn fd =
  ignore_result begin
    (* Printf.eprintf "run unix func: 0\n%!"; *)
    lwt () = func conn fd in
    (* Printf.eprintf "run unix func: 1\n%!"; *)
    close conn;
    (* Printf.eprintf "run unix func: 2\n%!"; *)
    lwt () =
      if Lwt_unix.state fd = Lwt_unix.Closed
      then return_unit
      else Lwt_unix.close fd
    in
    (* Printf.eprintf "run unix func: 3\n%!"; *)
    return_unit
  end

let run_unix_server
 (server : ('req, 'resp, [> `Bidi] as 'k) server)
 sock_domain sock_type proto sock_addr
 ?(listen = 5)
 (func : ('req, 'resp, 'k) unix_func)
 =
  let sock = Lwt_unix.socket sock_domain sock_type proto in
  Lwt_unix.setsockopt sock Unix.SO_REUSEADDR true;
  Lwt_unix.bind sock sock_addr;
  Lwt_unix.listen sock listen;
  let shutdown_waiter =
    lwt () = server.sctl.sc_shtd_waiter in
    return `Shutdown
  in
  ignore_result begin try_lwt begin
    let rec loop () =
      lwt ready = nchoose
        [ begin
            lwt (fd, _addr) = Lwt_unix.accept sock in
            return (`Accepted fd)
          end
        ; shutdown_waiter
        ] in
      let stop_now =
        List.fold_left
          (fun stop_now -> function
           | `Accepted fd ->
               let conn = connect ~ack_resp:true ~ack_req:false server in
               run_unix_func func conn fd;
               stop_now
           | `Shutdown ->
               true
          )
          false
          ready
      in
        if stop_now
        then return_unit
        else loop ()
    in
      loop ()
  end
  with e ->
    Printf.eprintf "run unix server exn: %s\n%!"
      (Printexc.to_string e);
    return_unit
  finally
    Lwt_unix.close sock
end

let ch_of_fd fd =
  (Lwt_io.of_fd ~mode:Lwt_io.input fd, Lwt_io.of_fd ~mode:Lwt_io.output fd)

let don't_setup_fd (_ : Lwt_unix.file_descr) = return_unit

let do_nothing_on_server_close
 (_ : Lwt_io.input_channel)
 (_ : Lwt_io.output_channel)
 (_ : exn)
  =
   return_unit

(* req_of_bytes can throw End_of_file to indicate closing *)
let unix_func_of_maps
 ?(setup_fd = don't_setup_fd)
 ?(on_server_close = do_nothing_on_server_close)
 req_from_inch resp_to_outch =
  fun conn fd ->
    try_lwt
      lwt () = setup_fd fd in
      let (inch, outch) = ch_of_fd fd in
      let make_inch_reader () =
        lwt req_opt =
          try_lwt
            lwt req = req_from_inch inch in
            return @@ Some req
          with
            End_of_file -> return_none
        in
        return @@ `From_inch req_opt
      and make_conn_reader () =
        lwt resp_res = recv_res_ack conn in
        return @@ `From_conn resp_res
      in
      let rec loop ths =
        (* Printf.eprintf "loop, ths=%i\n%!" (List.length ths); *)
        if ths = []
        then
          return_unit
        else
          lwt (ready, running) = nchoose_split ths in
          lwt ths =
            Lwt_list.fold_left_s begin
              fun ths -> function
              | `From_inch (Some req) ->
                  lwt () = send conn req in
                  return @@ make_inch_reader () :: ths
              | `From_conn (`Ok (resp, cfm)) ->
                  lwt send_res =
                    try_lwt
                      lwt () = resp_to_outch outch resp in
                      return_none
                    with
                      e -> return @@ Some e
                  in
                  begin match send_res with
                  | None ->
                      ack cfm;
                      return @@ make_conn_reader () :: ths
                  | Some exn ->
                      nack cfm exn;
                      lwt () = Lwt_io.abort inch in
                      close conn ~exn;
                      return_nil
                  end
              | `From_inch None ->
                  shutdown conn Unix.SHUTDOWN_SEND;
                  return ths
              | `From_conn (`Error exn) ->
                  lwt () = on_server_close inch outch exn in
                  lwt () =
                    if Lwt_unix.state fd = Lwt_unix.Opened
                    then
                      lwt () = Lwt_io.flush outch in
                      Lwt_unix.shutdown fd Unix.SHUTDOWN_ALL;
                      return_unit
                    else
                      return_unit
                  in
                  return_nil
                  (* fd will be closed by run_unix_server *)
            end
            running
            ready
          in
            loop ths
      in
        loop [make_inch_reader (); make_conn_reader ()]
    with e ->
      Printf.eprintf "unix func of maps: %s\n%!"
        (Printexc.to_string e);
      return_unit

let connect_unix_on_close = Lazy.from_val ()

let connect_unix unix_func sock_domain sock_type proto sock_addr =
  let sock = Lwt_unix.socket sock_domain sock_type proto in
  lwt () = Lwt_unix.connect sock sock_addr in
  let (server_conn, client_conn) =
    conn_pair ~ack_req:true ~ack_resp:false connect_unix_on_close in
  run_unix_func unix_func server_conn sock;
  return client_conn
