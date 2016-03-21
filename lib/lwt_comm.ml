open Lwt
module M = Lwt_mq_map
module Cond = Lwt_condition

type link_state =
  | St_ready
  | St_waiting_ack
  | St_closed of exn

type ack_sync =
  | No_acks
  | Acks of Lwt_mutex.t * exn option Lwt_condition.t
      (** outer mutex, ack condvar *)

type -'a mq_sink =
  { si : 'a M.sink
  ; si_acks : ack_sync
  ; si_state : link_state ref
  }

type +'a mq_source =
  { so : 'a M.source
  ; so_acks : ack_sync
  ; so_state : link_state ref
  }

type (-'snd, +'rcv, 'kind) conn =
  { snd_sink : 'snd mq_sink
  ; rcv_source : 'rcv mq_source
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

let log_ str =
  Printf.eprintf "%i: %s\n%!" (Unix.getpid ()) str

let log fmt =
  Printf.ksprintf log_ fmt

let log_exn exn fmt =
  Printf.ksprintf begin fun s ->
      log "%s: %s" s (Printexc.to_string exn);
    end
    fmt

let conn_names = ref []

let conn_name c =
  try
    List.assq c.on_close !conn_names
  with
    Not_found -> "<nameless>"

let set_conn_name c (n : string) =
  let n = "[" ^ n ^ "]" in
  try
    conn_names := (c.on_close, List.assq c.on_close !conn_names ^ "=" ^ n)
      :: !conn_names
  with Not_found ->
    conn_names := (c.on_close, n) :: !conn_names

let ack conn =
  let s = conn.rcv_source in
  let st = s.so_state in
  match !st, s.so_acks with
  | St_ready, Acks _ -> invalid_arg "Lwt_comm.ack: already ACKed"
  | St_waiting_ack, No_acks -> assert false
  | St_closed _exn, Acks _ -> invalid_arg "Lwt_comm.ack: connection closed"
  | St_closed _exn, No_acks -> ()
  | St_ready, No_acks -> ()
  | St_waiting_ack, Acks (_o, a) ->
      st := St_ready;
      Lwt_condition.signal a None

let send conn msg =
  let s = conn.snd_sink in
  let acks = s.si_acks in
  let st = s.si_state in
  match acks with
  | No_acks -> begin
      match !st with
      | St_waiting_ack -> assert false
      | St_closed exn -> fail exn
      | St_ready -> M.sink_put s.si msg
    end
  | Acks (o, a) ->
      Lwt_mutex.with_lock o @@ fun () ->
        match !st with
        | St_waiting_ack -> assert false
        | St_closed exn -> fail exn
        | St_ready ->
            st := St_waiting_ack;
            let reset_st () = if !st = St_waiting_ack then st := St_ready in
            let wait_ack = Lwt_condition.wait a in
            on_cancel wait_ack reset_st;
            let put = M.sink_put s.si msg in
            on_cancel put (fun () -> reset_st (); cancel wait_ack);
            lwt () = put in
            (* waiting; [ack] and [close] signal it *)
            match_lwt wait_ack with
            | None -> return_unit
            | Some exn -> fail exn

let ok_unit = `Ok ()
let ret_ok_unit = fun () -> return ok_unit

let send_res_counter = ref 0

let send_res conn msg =
  catch
    (fun () ->
       incr send_res_counter;
       (*- Printf.eprintf "send_res: %s %i: before\n%!"
         (conn_name conn) !send_res_counter; -*)
       send conn msg >>= ret_ok_unit
       (*- >|= fun r -> Printf.eprintf "send_res: %s %i: ok\n%!"
                      (conn_name conn) !send_res_counter; r -*)
    )
    (fun e ->
       (*- Printf.eprintf "send_res: %s %i: error\n%!"
         (conn_name conn) !send_res_counter; -*)
       return (`Error e)
    )

let recv_no_ack conn =
  (*- let () = Printf.eprintf "DBG: recv_no_ack %s\n%!" (conn_name conn) in -*)
  let s = conn.rcv_source in
  try_lwt
    M.source_take s.so
    (*- >|= (fun m ->
      Printf.eprintf "DBG: recv_no_ack %s done\n%!" (conn_name conn); m) -*)
  with
    Lwt_mq.Closed e ->
      (*- Printf.eprintf "DBG: recv_no_ack %s error: %s\n%!" (conn_name conn)
        (Printexc.to_string e); -*)
      fail e

let recv_no_ack_res conn =
  (*- let () = Printf.eprintf "DBG: recv_no_ack_res %s\n%!"
    (conn_name conn) in -*)
  try_lwt
    lwt r = recv_no_ack conn in
    return @@ `Ok r
  with
    e -> return @@ `Error e

let recv_no_ack_opt conn =
  (*- let () = Printf.eprintf "DBG: recv_no_ack_opt\n%!" in -*)
  try
    lwt r = recv_no_ack conn in
    return @@ Some r
  with
    End_of_file -> return_none

let recv conn =
  (*- let () = Printf.eprintf "DBG: recv\n%!" in -*)
  lwt r = recv_no_ack conn in
  ack conn;
  return r

let recv_opt conn =
  (*- let () = Printf.eprintf "DBG: recv_opt\n%!" in -*)
  lwt r = recv_no_ack_opt conn in
  (match r with None -> () | Some _ -> ack conn);
  return r

let recv_res conn =
  (*- let () = Printf.eprintf "DBG: recv_res %s\n%!" (conn_name conn) in -*)
  try_lwt
    lwt r = recv_no_ack conn in
    (*- let () = Printf.eprintf "DBG: recv_res %s: before ack\n%!"
      (conn_name conn) in -*)
    ack conn;
    (*- let () = Printf.eprintf "DBG: recv_res %s: after ack\n%!"
      (conn_name conn) in -*)
    return @@ `Ok r
  with
    e ->
      (*- Printf.eprintf "DBG: recv_res %s: %s\n%!" (Printexc.to_string e)
        (conn_name conn); -*)
      return @@ `Error e

let is_state_closed st =
  match !st with
  | St_closed _ -> true
  | St_ready | St_waiting_ack -> false

let run_on_close conn =
  if    is_state_closed conn.snd_sink.si_state
     && is_state_closed conn.rcv_source.so_state
  then Lazy.force conn.on_close
  else ()

let shutdown_sd exn conn =
  (*- let () = Printf.eprintf "DBG: shutdown_sd %s\n%!" (conn_name conn) in -*)
  let sink = conn.snd_sink in
  let st = sink.si_state in
  match !st with
  | St_closed _ -> ()
  | St_waiting_ack | St_ready ->
      st := St_closed exn;
      M.close_sink sink.si exn;
      run_on_close conn

let shutdown_rc exn conn =
  (*- let () = Printf.eprintf "DBG: shutdown_rc %s\n%!" (conn_name conn) in -*)
  let source = conn.rcv_source in
  let st = source.so_state in
  match !st with
  | St_closed _ -> ()
  | St_waiting_ack | St_ready ->
      source.so_state := St_closed exn;
      (* signal to sender that waits for ACK (if any) *)
      begin match source.so_acks with
      | No_acks -> ()
      | Acks (_o, a) -> Lwt_condition.signal a (Some exn)
      end;
      (* kill (on_recv:true) all other senders and close source *)
      M.close_source source.so exn ~on_recv:true;
      run_on_close conn

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

type _ wait =
  | Time : float -> [ `Instances_exist of int | `Shut_down ] wait
  | Forever : unit wait
  | Don't : int wait

let shutdown_server_wait ?(exn = Server_shut_down)
  sctl (type a) (wait : a wait) : a Lwt.t =
  let shutdown_return () =
    log_exn exn "closing server";
    sctl.sc_state <- Ss_closed exn;
    Lazy.force sctl.sc_shtd;
    lwt () = sctl.sc_shtd_waiter in  (* can be cancelled at [on_shutdown ()] *)
    return (begin
      match wait with
      | Time _ -> `Shut_down
      | Forever -> ()
      | Don't -> sctl.instances (* = 0 *)
    end : a)
  in
  if sctl.instances = 0
  then
    shutdown_return ()
  else
    let do_wait timeout_waiter =
      try_lwt
        let (cond, close_waiter_ref) =
          match sctl.sc_state with
          | Ss_closed _exn -> assert false
              (* if server is closed, instances=0,
                 this if-branch is unreachable. *)
          | Ss_running ->
              let cond = Lwt_condition.create () in
              let close_waiter_ref = ref (Lwt_condition.wait cond) in
              sctl.sc_state <- Ss_closing (exn, cond, close_waiter_ref);
              (cond, close_waiter_ref)
          | Ss_closing (_old_exn, cond, close_waiter_ref) ->
              sctl.sc_state <- Ss_closing (exn, cond, close_waiter_ref);
              (cond, close_waiter_ref)
        in
        let rec loop () =
          let insts = sctl.instances in
          assert (insts > 0);
          (* Printf.eprintf "shutdown_server_wait loop, %i more instances\n%!"
               insts; *)
          let close_waiter = !close_waiter_ref >|= fun () -> `Conn_closed in
          match_lwt choose [timeout_waiter; protected close_waiter] with
          | `Timeout -> return begin
              match wait with
              | Don't -> assert false
              | Forever -> assert false
              | Time _ -> (`Instances_exist insts : a)
            end
          | `Conn_closed ->
              if insts = 0
              then begin
                shutdown_return ()
              end else begin
                close_waiter_ref := Lwt_condition.wait cond;
                loop ()
              end
        in
          loop ()
      finally
        cancel timeout_waiter;
        return_unit
    in
    match wait with
    | Don't -> return (sctl.instances : a)
    | Time t ->
        if t < 0.
        then fail @@ Invalid_argument
          "Lwt_comm.shutdown_server_wait: negative timeout"
        else
          do_wait (Lwt_unix.sleep t >|= fun () -> `Timeout)
    | Forever -> do_wait (let (wai, _wak) = task () in wai)

let shutdown_server ?(exn = Server_shut_down) sctl =
  log_exn exn
    "shutdown_server: conns: %i instances: %i\n%!"
    (Hashtbl.length sctl.conns) sctl.instances;
  Hashtbl.iter (fun _cid closefunc -> closefunc exn) sctl.conns;
  Hashtbl.clear sctl.conns;
  shutdown_server_wait ~exn sctl Forever

let shutdown_server_wait_infinite ?(exn = Server_shut_down) sctl =
  shutdown_server_wait ~exn sctl Forever

let wait_for_server_shutdown sctl =
  protected sctl.sc_shtd_waiter

let run_lwt_server server server_conn =
  let ctl = server.sctl in
  try_lwt
    ctl.instances <- ctl.instances + 1;
    lwt () = server.shandler server_conn in
    close server_conn;
    return_unit
  with exn ->
    (*- Printf.eprintf "server/error: before close\n%!"; -*)
    close server_conn ~exn;
    (*- Printf.eprintf "server/error: after close\n%!"; -*)
    return_unit
  finally
    ctl.instances <- ctl.instances - 1;
    begin match ctl.sc_state with
    | Ss_running | Ss_closed _ -> ()
    | Ss_closing (_exn, cond, _close_waiter) ->
        Lwt_condition.signal cond ()
    end;
    return_unit

let si_so_pair ~block_limit =
  M.sink_source @@ M.of_mq @@ Lwt_mq.create ~block_limit ()

let si_so_link acks =
  let block_limit =
    if acks
    then 0  (* no values are stored.  every sender waits for [recv]. *)
    else Lwt_mq.no_limit
  in
  let acks =
    if acks
    then Acks (Lwt_mutex.create (), Lwt_condition.create ())
    else No_acks
  in
  let closed = ref St_ready in
  let (si, so) = si_so_pair ~block_limit in
  ( { si = si
    ; si_state = closed
    ; si_acks = acks
    }
  , { so = so
    ; so_state = closed
    ; so_acks = acks
    }
  )

let conn_pair ~ack_req ~ack_resp on_close =
  let (s2c_sink, s2c_source) = si_so_link ack_resp
  and (c2s_sink, c2s_source) = si_so_link ack_req in
  let server_conn =
    { snd_sink = s2c_sink
    ; rcv_source = c2s_source
    ; on_close = on_close
    }
  and client_conn =
    { snd_sink = c2s_sink
    ; rcv_source = s2c_source
    ; on_close = on_close
    }
  in
  (server_conn, client_conn)

let map_sink m s =
  { si = M.map_sink m s.si
  ; si_acks = s.si_acks
  ; si_state = s.si_state
  }

let map_source m s =
  { so = M.map_source m s.so
  ; so_acks = s.so_acks
  ; so_state = s.so_state
  }

let map_conn map_req map_resp conn =
  { snd_sink = map_sink map_req conn.snd_sink
  ; rcv_source = map_source map_resp conn.rcv_source
  ; on_close = conn.on_close
  }
  (*- |> fun c -> set_conn_name c (conn_name conn ^ "/mapped"); c -*)

let map_server map_req map_resp server =
  let old_shandler = server.shandler in
  { server with shandler = fun conn ->
      let conn' = map_conn map_resp map_req conn in
      old_shandler conn'
  }

let next_conn_id =
  let cur = ref 0 in
  fun () -> (incr cur; !cur)

let async place func =
  Lwt.async begin fun () ->
    try_lwt
      func ()
    with exn ->
      log_exn exn
        "uncaught exception from Lwt_comm.%s background thread"
        place;
      return_unit
  end

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
      async "connect" begin fun () ->
        run_lwt_server server server_conn
      end;
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
  async "run_unix_func" begin fun () ->
    try_lwt
      (*- Printf.eprintf "run unix func: 0\n%!"; -*)
      lwt () =
        try_lwt
          func conn fd
        finally
          (*- Printf.eprintf "run unix func: 1\n%!"; -*)
          close conn;
          return_unit
      in
      (*- Printf.eprintf "run unix func: 2\n%!"; -*)
      return_unit
    finally
      if Lwt_unix.state fd = Lwt_unix.Closed
      then
        return_unit
      else
        try_lwt Lwt_unix.close fd with _ (* todo logging *) -> return_unit
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
    lwt () = wait_for_server_shutdown server.sctl in
    return `Shutdown
  in
  async "run_unix_server" begin fun () -> try_lwt begin
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
    log_exn e "run unix server";
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
        lwt resp_res = recv_no_ack_res conn in
        return @@ `From_conn resp_res
      in
      let rec loop ths =
        (*- Printf.eprintf "loop, ths=%i\n%!" (List.length ths); -*)
        if ths = []
        then
          return_unit
        else
          lwt (ready, running) = nchoose_split ths in
          lwt ths =
            Lwt_list.fold_left_s begin
              fun ths -> function
              | `From_inch (Some req) ->
                  (*- Printf.eprintf "unix_func_of_maps: inch/some\n%!"; -*)
                  lwt () = send conn req in
                  return @@ make_inch_reader () :: ths
              | `From_conn (`Ok resp) ->
                  (*- Printf.eprintf "unix_func_of_maps: conn/resp\n%!"; -*)
                  lwt send_res =
                    try_lwt
                      lwt () = resp_to_outch outch resp in
                      return_none
                    with
                      e -> return @@ Some e
                  in
                  begin match send_res with
                  | None ->
                      ack conn;
                      return @@ make_conn_reader () :: ths
                  | Some exn ->
                      lwt () = Lwt_io.abort inch in
                      close conn ~exn;
                      return_nil
                  end
              | `From_inch None ->
                  (*- Printf.eprintf "unix_func_of_maps: inch/none\n%!"; -*)
                  shutdown conn Unix.SHUTDOWN_SEND;
                  return ths
              | `From_conn (`Error exn) ->
                  (*- Printf.eprintf "unix_func_of_maps: conn/err\n%!"; -*)
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
      log_exn e "unix func of maps";
      return_unit

let connect_unix_on_close = Lazy.from_val ()

let connect_unix unix_func sock_domain sock_type proto sock_addr =
  let sock = Lwt_unix.socket sock_domain sock_type proto in
  lwt () = Lwt_unix.connect sock sock_addr in
  let (server_conn, client_conn) =
    conn_pair ~ack_req:true ~ack_resp:false connect_unix_on_close in
  run_unix_func unix_func server_conn sock;
  return client_conn

let stream_next_opt s =
  try Some (Stream.next s) with Stream.Failure -> None

let rec join_early_fail (lst : unit Lwt.t list) : unit Lwt.t =
  if lst = []
  then return_unit
  else
    lwt (ready, running) = nchoose_split lst in
    assert (ready <> []);
    join_early_fail running

let reconnecting_server make_timeouts_stream (inner_server, inner_ctl) =
  duplex begin fun client_conn ->
    (*-
    set_conn_name client_conn "client<->reconnector";
    let conn_num = ref 0 in
    -*)

    let make_server_conn () =
      let c = connect inner_server ~ack_req:true ~ack_resp:false in
      (*-
      incr conn_num;
      set_conn_name c @@ Printf.sprintf "reconnector<->server#%i" !conn_num;
      -*)
      return @@ `Conn c
    in
    let server_conn_ref = ref (make_server_conn ()) in

    (* locked when reconnecting; unlocked -> server_conn_ref = Some _ *)
    let conn_mut = Lwt_mutex.create () in

    let get_server_conn () =
      if Lwt_mutex.is_locked conn_mut
      then
        Lwt_mutex.with_lock conn_mut @@ fun () -> !server_conn_ref
      else
        !server_conn_ref
    in

    let timeouts_stream = make_timeouts_stream () in

    let reconnect =
      let reconn_thread = ref None in
      let s2c_reconn_cond = Lwt_condition.create () in
      fun initiator e ->
        match initiator, !reconn_thread with
        | (`C2S | `S2C), Some th ->
            (*- Printf.eprintf "re: joining reconn thread\n%!"; -*)
            th
        | `C2S, None ->
            (* before reconnecting initiated from `C2S (error sending client
               message to server): receive all messages sent by server: don't
               reconnect now. *)
            (*- Printf.eprintf "re: c2s: waiting on cond\n%!"; -*)
            Lwt_condition.wait s2c_reconn_cond
        | `S2C, None ->
            let th = Lwt_mutex.with_lock conn_mut @@ fun () ->
              (*- Printf.eprintf "re: start reconnecting\n%!"; -*)
              lwt () =
                match stream_next_opt timeouts_stream with
                | None ->
                    server_conn_ref := (return @@ `Stopped e);
                    return_unit
                | Some t ->
                    server_conn_ref := (return `Reconnecting);
                    lwt () = Lwt_unix.sleep t in
                    server_conn_ref := make_server_conn ();
                    (*- Printf.eprintf "re: reconnected\n%!"; -*)
                    return_unit
              in
                Lwt_condition.signal s2c_reconn_cond ();
                reconn_thread := None;
                return_unit
            in
              reconn_thread := Some th;
              th
    in

    let rec client_to_server () =
      match_lwt recv_no_ack_res client_conn with
      | `Error exn -> begin
          (*- Printf.eprintf "re/c2s: error from client: %s\n%!"
            (Printexc.to_string exn); -*)
          shutdown ~exn client_conn Unix.SHUTDOWN_RECEIVE;
          match_lwt get_server_conn () with
          | `Stopped _exn -> return_unit
          | `Reconnecting -> assert false
          | `Conn server_conn ->
              (*- Printf.eprintf "re/c2s: shutdown_send server_conn\n%!"; -*)
              shutdown server_conn ~exn Unix.SHUTDOWN_SEND;
              server_conn_ref := (return @@ `Stopped exn);
              return_unit
        end
      | `Ok msg ->
          let rec send_to_server () =
            (*- Printf.eprintf "re/c2s: send loop\n%!"; -*)
            match_lwt get_server_conn () with
            | `Reconnecting -> assert false
            | `Stopped exn ->
                shutdown client_conn ~exn Unix.SHUTDOWN_RECEIVE;
                return `Stop
            | `Conn server_conn ->
                (*- Printf.eprintf "re/c2s: send loop `Conn %s : %i\n%!"
                  (conn_name server_conn) (Obj.magic msg); -*)
                match_lwt send_res server_conn msg with
                | `Ok () -> ack client_conn; return `Continue
                | `Error exn ->
                    (*- Printf.eprintf
                      "re/c2s: send loop: error sending to %s\n%!"
                      (conn_name server_conn); -*)
                    shutdown server_conn ~exn Unix.SHUTDOWN_SEND;
                    reconnect `C2S exn >>= send_to_server
          in
          match_lwt send_to_server () with
          | `Continue ->
              (*- Printf.eprintf "re/c2s: cont\n%!"; -*)
              client_to_server ()
          | `Stop ->
              (*- Printf.eprintf "re/c2s: stop\n%!"; -*)
              return_unit
    in

    let rec server_to_client () =
      let rec recv_from_server () =
        (*- Printf.eprintf "re/s2c: recv loop\n%!"; -*)
        match_lwt get_server_conn () with
        | `Reconnecting -> assert false
        | `Stopped exn ->
            (*- Printf.eprintf "re/s2c: recv loop: `Stopped\n%!"; -*)
            shutdown client_conn ~exn Unix.SHUTDOWN_SEND;
            return `Stop
        | `Conn server_conn ->
            (*- Printf.eprintf "re/s2c: recv loop: `Conn %s\n%!"
              (conn_name server_conn); -*)
            match_lwt recv_res server_conn with
            | (`Ok _msg) as r ->
                (*- Printf.eprintf "re/s2c: recv loop: `Conn %s / `Ok\n%!"
                  (conn_name server_conn); -*)
                return r
            | `Error exn ->
                (*- Printf.eprintf "re/s2c: recv loop: `Conn %s / `Error\n%!"
                  (conn_name server_conn); -*)
                match_lwt get_server_conn () with
                | `Reconnecting -> assert false
                | `Stopped _exn -> return `Stop
                | `Conn server_conn ->
                    shutdown server_conn ~exn Unix.SHUTDOWN_ALL;
                    reconnect `S2C exn >>= recv_from_server
      in
      match_lwt recv_from_server () with
      | `Stop -> return_unit
      | `Ok msg ->
          (*- Printf.eprintf "re/s2c: received from server, sending to %s\n%!"
            (conn_name client_conn); -*)
          match_lwt send_res client_conn msg with
          | `Ok () ->
              (*- Printf.eprintf "re/s2c: sent to client ok\n%!"; -*)
              server_to_client ()
          | `Error exn ->
              (*- Printf.eprintf "re/s2c: error sending to client\n%!"; -*)
              shutdown client_conn ~exn Unix.SHUTDOWN_SEND;
              match_lwt get_server_conn () with
              | `Reconnecting -> assert false
              | `Stopped _exn -> return_unit
              | `Conn server_conn ->
                  shutdown server_conn ~exn Unix.SHUTDOWN_ALL;
                  return_unit
    in
      join_early_fail [ client_to_server () ; server_to_client () ]
(*
        [ (try_lwt client_to_server ()
           with e -> Printf.eprintf "c2s error\n%!"; fail e
          )
        ; (try_lwt server_to_client ()
           with e -> Printf.eprintf "s2c error\n%!"; fail e
          )
        ]
*)
  end
  ~on_shutdown: begin fun () ->
    lwt () = shutdown_server inner_ctl in
    return_unit
  end


let res_of_exn func =
  try `Ok (func ()) with e -> `Error e

let switch (type k) ?(key_compare = Pervasives.compare)
 ~split ~combine make_conn =
  let module Key =
    struct
      type t = k
      let compare = key_compare
    end
  in
  let module Kmap = Map.Make(Key) in
  let kmap_find_opt k m = try Some (Kmap.find k m) with Not_found -> None in
  let ret_cont = return `Continue in
  duplex begin fun client_conn ->
    let kmap = ref Kmap.empty in
    let closed_client_on_send = ref None in
    let client_closed_on_recv = ref false in
    let close_client_on_send exn =
      closed_client_on_send := Some exn
    in
    let close_s2c_loops exn =
      Kmap.iter (fun _key c -> close ~exn c) !kmap;
      kmap := Kmap.empty
    in
    let stop_switch = Lwt_condition.create () in
    let s2c_sender_mq = Lwt_mq.create ~block_limit:0 () in
    let s2c_sender_exit exn =
      (*- Printf.eprintf "sw: s2c_sender_loop: exitting with %s\n%!"
        (Printexc.to_string exn); -*)
      shutdown client_conn ~exn Unix.SHUTDOWN_SEND;
      Lwt_mq.close s2c_sender_mq exn ~on_recv:true;
      Lwt_condition.signal stop_switch ();
      return_unit
    in
    let rec s2c_sender_loop () =
      (*- Printf.eprintf "sw: s2c_sender_loop: entered, mq: %s\n%!"
        (Lwt_mq.dump s2c_sender_mq); -*)
      match_lwt Lwt_mq.take_res s2c_sender_mq with
      | `Ok msg -> begin
           (*- Printf.eprintf "sw: s2c_sender_loop: msg from mq\n%!"; -*)
           match_lwt send_res client_conn msg with
           | `Ok () ->
               (*- let (k, s) = Obj.magic msg in
               Printf.eprintf "sw: s2c_sender_loop: sent: (%i, %S)\n%!" k s;
               -*)
               s2c_sender_loop ()
           | `Error exn -> s2c_sender_exit exn
        end
      | `Error (Lwt_mq.Closed exn | exn) -> s2c_sender_exit exn
    in
    let s2c_loop key inner_conn =
      let rec loop () =
        (*- Printf.eprintf "sw: s2c_loop %i: entered\n%!" (Obj.magic key); -*)
        if !closed_client_on_send <> None
        then
          return_unit
        else
          lwt inner_resp_res = recv_no_ack_res inner_conn in
          (*- Printf.eprintf "sw: s2c_loop %i: recvd inner\n%!"
               (Obj.magic key); -*)
          begin match inner_resp_res, !closed_client_on_send with
          | `Ok _, None -> ack inner_conn
          | `Error exn, _ | _, Some exn ->
              (*- Printf.eprintf "sw: s2c_loop %i: nack\n%!"
                (Obj.magic key); -*)
              shutdown ~exn inner_conn Unix.SHUTDOWN_RECEIVE
          end;
          if !closed_client_on_send <> None
          then
            return_unit
          else begin
            let outer_resp_res = combine key inner_resp_res in
            match outer_resp_res with
            | `Ok outer_resp -> begin
                (*- Printf.eprintf "sw: s2c_loop %i: outer=Ok\n%!"
                  (Obj.magic key); -*)
                match_lwt Lwt_mq.put_res s2c_sender_mq outer_resp with
                | `Ok () -> begin
                    (*- Printf.eprintf "sw: s2c_loop %i: sent Ok\n%!"
                      (Obj.magic key); -*)
                    match inner_resp_res with
                    | `Ok _ -> loop ()
                    | `Error _ -> return_unit
                  end
                | `Error exn ->
                    (*- Printf.eprintf
                      "sw: s2c_loop %i: sent with error: %s\n%!"
                      (Obj.magic key) (Printexc.to_string exn); -*)
                    close_s2c_loops exn;
                    return_unit
              end
            | `Error exn ->
                (*- Printf.eprintf "sw: s2c_loop %i: outer=Er\n%!"
                  (Obj.magic key); -*)
                close_client_on_send exn;
                return_unit
          end
      in
        lwt () =
          try_lwt
            loop ()
          with
            exn ->
              (*- Printf.eprintf "switch/s2c exn\n%!"; -*)
              close ~exn inner_conn; return_unit
        in
        kmap := Kmap.remove key !kmap;
        return_unit
    in
    async "switch/s2c_sender_loop" s2c_sender_loop;
    let stop_switch_waiter = Lwt_condition.wait stop_switch in
    let rec loop () =
      begin
      match_lwt recv_no_ack_res client_conn with
      | `Ok outer_req -> begin
          match res_of_exn @@ fun () -> split outer_req with
          | `Ok (key, inner_req) -> begin
              let inner_conn =
                match kmap_find_opt key !kmap with
                | None ->
                    let c = make_conn key in
                    async "switch/s2c_loop" begin fun () ->
                      s2c_loop key c
                    end;
                    kmap := Kmap.add key c !kmap;
                    c
                | Some c -> c
              in
              match_lwt send_res inner_conn inner_req with
              | `Ok () -> ack client_conn; ret_cont
              | `Error exn ->
                  return @@ `Stop exn
          end
        | `Error exn -> return @@ `Stop exn
        end
      | `Error exn -> return @@ `Stop exn
      end >>= function
      | `Continue ->
          (*- Printf.eprintf "switch/c2s: continue\n%!"; -*)
          loop ()
      | `Stop exn ->
          (*- Printf.eprintf "switch/c2s: stopping with %s\n%!"
            (Printexc.to_string exn); -*)
          client_closed_on_recv := true;
          Kmap.iter (fun _k c -> shutdown ~exn c Unix.SHUTDOWN_SEND) !kmap;
          return_unit
    in
      (*- try_lwt -*)
      lwt () = loop () in
      begin try
        Lwt_mq.close s2c_sender_mq End_of_file
      with
        Lwt_mq.Closed _ -> ()
      end;
      lwt () = stop_switch_waiter in
      return_unit
      (*- with _ -> Printf.eprintf "switch exn\n%!"; return_unit -*)
  end

(* TEMP *)

let wait_group name ths =
  log "wait_group %S, %i threads" name (List.length ths);
  let ths = List.mapi begin fun i th ->
      let th =
        try_lwt
          lwt () = th in
          log "wait_group %S, thread #%i exited ok" name i;
          return (i, `Ok)
        with exn ->
          log_exn exn "wait_group %S, thread #%i failed" name i;
          return (i, `Error exn)
      in
        th
    end
    ths
  in
  try_lwt
    lwt (ready, res) = choose ths in
    log "wait_group %S, thread #%i finished: %s" name ready
      (match res with `Ok -> "ok" | `Error exn -> Printexc.to_string exn);
    List.iter cancel ths;
    fail (Failure (Printf.sprintf "wait_group %S: stopped" name))
  with exn ->
    log_exn exn "wait_group %S loop" name;
    fail exn
