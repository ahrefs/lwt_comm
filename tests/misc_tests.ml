open Lwt
open Lwt_comm
open Linenum_server_def

let send_close_recv () =
  let (server, _server_ctl) = make_linenum_server () in
  let conn = connect server ~ack_req:false ~ack_resp:false in
  lwt () = send conn "qwe" in
  shutdown conn Unix.SHUTDOWN_SEND;
  Printf.eprintf "scr1\n%!";
  lwt r = recv conn in
  Printf.eprintf "scr2\n%!";
  assert (r = "1: qwe");
  return_unit

let () = Lwt_main.run begin
  lwt () = send_close_recv () in
  print_endline "ok";
  return_unit
end
