open Lwt
open Lwt_comm
open Linenum_server_def

let send_close_recv () =
  let (server, _server_ctl) = make_linenum_server () in
  let conn = connect server ~ack_req:false ~ack_resp:false in
  lwt () = send conn "qwe" in
  shutdown conn Unix.SHUTDOWN_SEND;
  (* Printf.eprintf "scr1\n%!"; *)
  lwt r = recv conn in
  (* Printf.eprintf "scr2\n%!"; *)
  assert (r = "1: qwe");
  return_unit

let recv_error_ack () =
  let (server, _server_ctl) = make_linenum_server () in
  let conn = connect server ~ack_req:false ~ack_resp:false in
  lwt () = send conn "qwe" in
  shutdown conn Unix.SHUTDOWN_SEND;
  lwt r = recv conn in
  assert (r = "1: qwe");
  match_lwt recv_res conn with
  | `Ok _ -> assert false
  | `Error End_of_file -> return_unit
  | `Error _ -> assert false

let send_fail_ok () =
  let (fail_after_ack, _ctl) = duplex @@ fun conn ->
    lwt () = recv conn in
    fail Exit
  in
  let conn = connect fail_after_ack ~ack_req:true in
  send conn ()

let send_close_ok () =
  let (fail_after_ack, _ctl) = duplex @@ fun conn ->
    lwt () = recv conn in
    close conn ~exn:Exit;
    return_unit
  in
  let conn = connect fail_after_ack ~ack_req:true in
  send conn ()

let fail_before_ack () =
  let (server, _ctl) = duplex @@ fun conn ->
    lwt () = recv_no_ack conn in
    fail Exit
  in
  let c = connect server in
  try_lwt
    lwt () = send c () in
    assert false
  with
    Exit -> return_unit

let close_before_ack () =
  let (server, _ctl) = duplex @@ fun conn ->
    lwt () = recv_no_ack conn in
    close conn ~exn:Exit;
    return_unit
  in
  let c = connect server in
  try_lwt
    lwt () = send c () in
    assert false
  with
    Exit -> return_unit

let exit_before_ack () =
  let (server, _ctl) = duplex @@ fun conn ->
    lwt () = recv_no_ack conn in
    return_unit
  in
  let c = connect server in
  try_lwt
    lwt () = send c () in
    assert false
  with
    End_of_file -> return_unit

let () = Lwt_main.run begin
  lwt () =
    Lwt_list.iter_s
      (fun test -> test ())
      [ send_close_recv
      ; recv_error_ack
      ; send_fail_ok
      ; send_close_ok
      ; fail_before_ack
      ; close_before_ack
      ; exit_before_ack
      ; Reconnect_test.run
      ]
  in
  print_endline "Lwt_comm tests passed ok.";
  return_unit
end
