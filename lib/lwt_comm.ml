open Lwt

type 'a mq =
| Acks of ('a * unit Lwt.u) Lwt_mq.t
| No_acks of 'a Lwt_mq.t

type 'a link =
  { lmq : 'a mq
  ; mutable lclosed : bool
  }

type ('snd, 'rcv, 'kind) conn =
  { snd : 'snd link
  ; rcv : 'rcv link
  }

type server_state =
| Ss_running
| Ss_closed of exn

type server_ctl =
  { sc_shtd_waiter : unit Lwt.t
  ; sc_shtd : unit Lazy.t
  ; mutable sc_state : server_state
  }

type ('req, 'resp, 'kind) server =
  { shandler : ('resp, 'req, 'kind) conn -> unit Lwt.t
  ; sctl : server_ctl
  }

type 'snd confirmation = unit Lwt.u

let ack wak = wakeup wak ()

let nack wak exn = wakeup_exn wak exn

let send_link l s =
  match l.lmq with
  | Acks mq ->
      let (wai, wak) = wait () (* to think: cancellation *) in
      lwt () = Lwt_mq.put mq (s, wak) in
      wai
  | No_acks mq ->
      Lwt_mq.put mq s

let send conn s =
  send_link conn.snd s

let recv_ack conn =
  match conn.rcv.lmq with
  | Acks mq -> begin
      try_lwt
        Lwt_mq.take mq
      with
        Lwt_mq.Closed e -> fail e
    end
  | No_acks _ -> fail @@ Invalid_argument
      "Lwt_comm.recv_ack: connection doesn't need ACKs"

let recv conn =
  match conn.rcv.lmq with
  | Acks mq -> begin
      try_lwt
        lwt (r, cnf) = Lwt_mq.take mq in
        ack cnf;
        return r
      with
        Lwt_mq.Closed e -> fail e
    end
  | No_acks mq -> begin
      try_lwt
        Lwt_mq.take mq
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

let shutdown_link exn l =
  if l.lclosed
  then ()
  else begin
    l.lclosed <- true;
    (* try *)
      match l.lmq with
      | Acks mq -> Lwt_mq.close mq exn
      | No_acks mq -> Lwt_mq.close mq exn
    (*
    with e ->
      Printf.eprintf "close err: %s\n%!" (Printexc.to_string e);
      raise e
    *)
  end

let shutdown ?(exn = End_of_file) conn cmd =
  match cmd with
  | Unix.SHUTDOWN_SEND ->
      shutdown_link exn conn.snd
  | Unix.SHUTDOWN_RECEIVE ->
      shutdown_link exn conn.rcv
  | Unix.SHUTDOWN_ALL ->
      shutdown_link exn conn.snd;
      shutdown_link exn conn.rcv

let close ?(exn = End_of_file) conn =
  shutdown ~exn conn Unix.SHUTDOWN_ALL

let duplex serverfunc =
  let sctl =
    let (wt, wk) = Lwt.wait () in
    { sc_shtd_waiter = wt
    ; sc_shtd = lazy (Lwt.wakeup wk ())
    ; sc_state = Ss_running
    }
  in
  ( { shandler = serverfunc
    ; sctl = sctl
    }
  , sctl
  )

exception Server_shut_down

let shutdown_server ?(exn = Server_shut_down) sctl =
  sctl.sc_state <- Ss_closed exn;
  Lazy.force sctl.sc_shtd;
  sctl.sc_shtd_waiter

let wait_for_server_shutdown sctl =
  sctl.sc_shtd_waiter

let link acks =
  { lmq = if acks then Acks (Lwt_mq.create ()) else No_acks (Lwt_mq.create ());
    lclosed = false
  }

let run_lwt_server server server_conn =
  try_lwt
    lwt () = server.shandler server_conn in
    close server_conn;
    return_unit
  with exn ->
    close server_conn ~exn;
    return_unit

let connect
 : ?ack_req:bool -> ?ack_resp:bool ->
   ('req, 'resp, [> `Connect] as 'k) server ->
   ('req, 'resp, 'k) conn
 = fun ?(ack_req = true) ?(ack_resp = true) server ->
  match server.sctl.sc_state with
  | Ss_running ->
      let req_link = link ack_req
      and resp_link = link ack_resp in
      let server_conn = { snd = resp_link; rcv = req_link }
      and client_conn = { snd = req_link; rcv = resp_link } in
      Lwt.ignore_result (run_lwt_server server server_conn);
      client_conn
  | Ss_closed exn -> raise exn

let with_connection server func =
  let client_conn = connect server in
  try_lwt
    func client_conn
  finally
    close client_conn;
    return_unit

type ('req, 'resp, 'k) unix_func =
  ('req, 'resp, 'k) conn -> Lwt_unix.file_descr -> unit Lwt.t

let run_unix_server
 (server : ('req, 'resp, [> `Bidi] as 'k) server)
 sock_domain sock_type proto sock_addr
 ?(listen = 5)
 (func : ('req, 'resp, 'k) unix_func)
 =
  ignore_result begin try_lwt begin
    let sock = Lwt_unix.socket sock_domain sock_type proto in
    Lwt_unix.setsockopt sock Unix.SO_REUSEADDR true;
    Lwt_unix.bind sock sock_addr;
    Lwt_unix.listen sock listen;
    let shutdown_waiter =
      lwt () = server.sctl.sc_shtd_waiter in
      return `Shutdown
    in
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
               ignore_result begin
                 (* Printf.eprintf "run unix server: 0\n%!"; *)
                 lwt () = func conn fd in
                 (* Printf.eprintf "run unix server: 1\n%!"; *)
                 close conn;
                 (* Printf.eprintf "run unix server: 2\n%!"; *)
                 lwt () =
                   if Lwt_unix.state fd = Lwt_unix.Closed
                   then return_unit
                   else Lwt_unix.close fd
                 in
                 (* Printf.eprintf "run unix server: 3\n%!"; *)
                 return_unit
               end;
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
      lwt () = loop () in
      Lwt_unix.close sock
  end
  with e ->
    Printf.eprintf "run unix server exn: %s\n%!"
      (Printexc.to_string e);
    return_unit
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
