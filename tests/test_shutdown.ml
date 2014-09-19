open Printf
open Lwt
open Linenum_common
open Linenum_server_def

let test_string = "abc"

let () = Printexc.register_printer @@
  function
  | Lwt_mq.Closed e ->
      Some (sprintf "Lwt_mq.Closed (%s)" (Printexc.to_string e))
  | _ -> None

let client_loop conn =
  let rec loop i =
    if i > 10
    then begin
      C.close conn;
      return_unit
    end else begin
      eprintf "<%!";
      lwt () = C.send conn test_string in
      eprintf ".%!";
      lwt () = Lwt_unix.sleep 0.1 in
      eprintf "|%!";
      lwt r = C.recv conn in
      eprintf ">%!";
      assert (r = sprintf "%i: %s" i test_string);
      eprintf ".%!";
      lwt () = Lwt_unix.sleep 0.1 in
      loop (i + 1)
    end
  in
    try_lwt
      loop 1
    with e ->
      eprintf "client_loop exn: %s\n%!" (Printexc.to_string e);
      fail e

(* forced shutdown *)
let io_main1 () =
  let conn = C.connect linenum_server in
  ignore_result (client_loop conn);
  lwt () = Lwt_unix.sleep 0.5 in
  lwt () = C.shutdown_server server_ctl in
  lwt () = Lwt_unix.sleep 0.5 in
  return_unit

(* waiting for close with infinite timeout *)
let io_main2 () =
  let conn = C.connect linenum_server in
  ignore_result (client_loop conn);
  lwt () = Lwt_unix.sleep 0.5 in
  match_lwt C.shutdown_server_wait server_ctl with
  | `Shut_down -> eprintf "shut down\n%!"; return_unit
  | `Instances_exist n -> eprintf "instances exist: %i\n%!" n; return_unit

(* waiting for close with small timeout, then forcing shutdown *)
let io_main3 () =
  let conn = C.connect linenum_server in
  ignore_result (client_loop conn);
  lwt () = Lwt_unix.sleep 0.3 in
  match_lwt C.shutdown_server_wait server_ctl ~timeout:0.3 with
  | `Shut_down -> eprintf "shut down\n%!"; return_unit
  | `Instances_exist n ->
      eprintf "instances exist: %i, forcing shutdown\n%!" n;
      lwt () = C.shutdown_server server_ctl in
      return_unit

(* waiting for close with small timeout, then waiting once more with infinite
   timeout *)
let io_main4 () =
  let conn = C.connect linenum_server in
  ignore_result (client_loop conn);
  lwt () = Lwt_unix.sleep 0.3 in
  match_lwt C.shutdown_server_wait server_ctl ~timeout:0.3 with
  | `Shut_down -> eprintf "shut down #1\n%!"; return_unit
  | `Instances_exist n ->
      eprintf "instances exist: %i, waiting more\n%!" n;
      match_lwt C.shutdown_server_wait server_ctl with
      | `Shut_down -> eprintf "shut down #2\n%!"; return_unit
      | `Instances_exist n ->
          eprintf "instances exist: %i\n%!" n;
          return_unit

let () =
  try
    Lwt_main.run (io_main4 ())
  with
    e ->
      eprintf "exception: %s\n%!" (Printexc.to_string e);
      exit 1
