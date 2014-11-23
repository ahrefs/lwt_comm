open Lwt
open Lwt_comm

let nclients = 10

let msgs_per_client = 10

let fail_every_n = 3

let () = Random.init 0

exception Server_failure

let make_failing_server () =
  let n = ref 0 in
  let fail_before_ack = ref true in
  duplex @@ fun conn ->
    set_conn_name conn "failing_server";
    let rec loop () =
      Printf.eprintf "rc/server: receiving\n%!";
      match_lwt recv_no_ack_res conn with
      | `Ok msg ->
         Printf.eprintf "rc/server: received %i\n%!" msg;
         incr n;
         if !n = fail_every_n && !fail_before_ack
         then begin
           n := 0;
           fail_before_ack := false;
           Printf.eprintf "rc/server: failing before ack\n%!";
           fail Server_failure
         end else begin
           lwt () =
             try_lwt
               return @@ ack conn;
             with
               e -> fail @@
                 Failure ("failing_server/ack: " ^ Printexc.to_string e)
           in
           Printf.eprintf "rc/server: sending %i\n%!" msg;
           lwt () = send conn msg in
           Printf.eprintf "rc/server: sent %i\n%!" msg;
(*
           lwt () = Lwt_unix.sleep (0.001 +. Random.float 0.01) (* todo: opt *)
           in
*)
           if !n = fail_every_n && not !fail_before_ack
           then begin
             Printf.eprintf "rc/server: failing after ack\n%!";
             n := 0;
             fail_before_ack := true;
             fail Server_failure
           end else
             loop ()
         end
      | `Error _ -> assert false
    in
(*
      try_lwt
*)
        loop ()
(*
      with
        e -> fail @@ Failure ("failing server exn: " ^ Printexc.to_string e)
*)

let (server, _ctl) =
  if true
  then
    reconnecting_server
      (fun () -> Stream.from (fun _ -> Some 0.1))
      (make_failing_server ())
  else
    make_failing_server ()

let run_client wake_on_exit =
  let conn = connect server (* ~ack_resp:false *) in
  set_conn_name conn "client";
  let rec loop i =
    if i = msgs_per_client
    then begin
      shutdown conn Unix.SHUTDOWN_SEND;
      lwt () =
        Printf.eprintf "rc/client: reading eof from %s\n%!" (conn_name conn);
        match_lwt recv_res conn with
        | `Ok _ -> assert false
        | `Error End_of_file -> return_unit
        | `Error e -> fail e
      in
      Printf.eprintf "rc/client: eof was read\n%!";
      wakeup wake_on_exit ();
      return_unit
    end else begin
      (* printf "RQ:%i %!" i; *)
      lwt () =
        try_lwt
          Printf.eprintf "rc/client: sending %i to %s\n%!" i (conn_name conn);
          send conn i
          >|= (fun () ->
            Printf.eprintf "rc/client: sent %i to %s\n%!" i (conn_name conn))
        with e ->
          failwith @@ Printf.sprintf
            "error sending %i to server: %s" i (Printexc.to_string e)
      in
      Printf.eprintf "rc/client: receiving from %s\n%!" (conn_name conn);
      match_lwt recv_res conn with
      | `Ok r ->
          Printf.eprintf "rc/client: received %i from %s\n%!"
            r (conn_name conn);
          (* printf "RS:%i %!" r; *)
          if i <> r
          then
            failwith "bad reply"
          else
            (* lwt () = Lwt_unix.sleep 0.1 in *)
            loop (i + 1)
      | `Error e -> failwith ("error from server: " ^ Printexc.to_string e)
    end
  in
    try_lwt
      loop 0
    with
      e -> fail @@ Failure ("client exn: " ^ Printexc.to_string e)

let run_clients () =
  let rec make_clients i =
    if i = nclients
    then []
    else
      let (wai, wak) = wait () in
      ignore_result begin
        run_client wak
      end;
      wai :: make_clients (i + 1)
  in
  let clients = make_clients 0 in
  join clients

let () = Lwt_main.run begin
  lwt () = run_clients () in
  print_endline "done";
  return_unit
end
