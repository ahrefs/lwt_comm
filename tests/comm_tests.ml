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

let switch_ok () =
  let make_conn key =
    let (linenum_server, _ctl) = make_linenum_server () in
    let mapped_server =
      map_server
        (fun req -> req)
        (fun resp -> resp ^ "/" ^ string_of_int key)
        linenum_server
    in
    let c = connect mapped_server in
    set_conn_name c @@ "conn_for_key_" ^ string_of_int key;
    c
  in
  let (server, _ctl) = switch
    ~split:(fun ((_key, _str) as ks) -> ks)
    ~combine:(fun key -> function `Ok s -> `Ok (key, s) | `Error e -> `Error e)
    make_conn
  in
  let c = connect server in
  set_conn_name c "switch";
  let requests = [(1, "a"); (2, "b"); (1, "c")] in
  let expected_responses = [(1, "1: a/1"); (2, "1: b/2"); (1, "2: c/1")] in
  begin ignore_result @@
    lwt () = Lwt_list.iter_s (fun msg -> send c msg) requests in
    shutdown c Unix.SHUTDOWN_SEND;
    return_unit
  end;
  let rec recv_loop i exps =
    (*- let () = Printf.eprintf "switch_ok/recv_loop: entered %i\n%!" i in -*)
    match exps with
    | [] -> begin
        match_lwt recv_res c with
        | `Ok _ -> assert false
        | `Error End_of_file -> return_unit
        | `Error _ -> assert false
      end
    | exp :: exps ->
        match_lwt recv_res c with
        | `Ok got ->
            (*- let () = Printf.eprintf "got: %i %S\n%!"
               (fst got) (snd got) in -*)
            if exp = got
            then
              recv_loop (i + 1) exps
            else
              assert false
        | `Error _ -> assert false
  in
  recv_loop 0 expected_responses

exception My_exn

let switch_inner_close () =
  let make_conn key =
    let (inner_server, _ctl) = duplex @@ fun conn ->
      set_conn_name conn @@ "conn_for_key_" ^ string_of_int key;
      lwt v = recv conn in
      lwt () = send conn ("1: " ^ v ^ "/" ^ string_of_int key) in
      fail End_of_file
    in
    connect inner_server
  in
  let (server, _ctl) = switch
    ~split:(fun ((_key, _str) as ks) -> ks)
    ~combine:(fun key ->
      function
      | `Ok s -> `Ok (key, s)
      | `Error e -> `Ok (key, Printexc.to_string e)
    )
    make_conn
  in
  let c = connect server in
  set_conn_name c "switch";
  let requests = [(1, "a"); (2, "b")] in
  let expected_responses =
    [ (1, "1: a/1"); (2, "1: b/2")
    ; (1, "End_of_file"); (2, "End_of_file")
    ] in
  begin ignore_result @@
    try_lwt
      lwt () = Lwt_list.iter_s (fun msg -> send c msg) requests in
      lwt () = Lwt_unix.sleep 0.01 in
      shutdown c Unix.SHUTDOWN_SEND ~exn:My_exn;
      return_unit
    with _ -> failwith "switch_inner_close/sender"
  end;
  let rec recv_loop i ~acc =
    if i = List.length expected_responses
    then
      match_lwt recv_res c with
      | `Ok (_, s) -> failwith s
      | `Error End_of_file -> return @@ List.rev acc
      | `Error _ -> assert false
    else
      match_lwt recv_res c with
      | `Ok got ->
          (*- let () = Printf.eprintf "got: (%i, %S)\n%!"
            (fst got) (snd got) in -*)
          recv_loop (i + 1) ~acc:(got :: acc)
      | `Error e ->
          Printf.eprintf "switch_inner_close/recv_loop/`Error: %s\n%!"
            (Printexc.to_string e);
          assert false
  in
  lwt got_responses = recv_loop 0 ~acc:[] in
  if List.sort compare expected_responses =
     List.sort compare got_responses
  then
    return_unit
  else
    assert false

let thread_test () =
  let module P = Lwt_preemptive in
  let nthreads = ref 0 in
  let nreqs = ref 0 in
  let thread_func conn =
    let tid = Thread.(id @@ self ()) in
    (*- Printf.eprintf "thread_test: tid=%i\n%!" tid; -*)
    incr nthreads;
    let rec loop () =
      match P.run_in_main @@ fun () -> recv conn with
      | `Req t ->
          incr nreqs;
          Thread.delay t;
          decr nreqs;
          let () = P.run_in_main @@ fun () ->
            send conn (t, tid)
          in
          loop ()
      | `Stop ->
          ()
    in
      loop ()
  in
  let (server, _ctl) = duplex @@ begin fun conn ->
    lwt () = P.detach thread_func conn in
    decr nthreads;
    close conn;
    return_unit
  end in
  assert (!nthreads = 0);
  let c1 = connect server in
  lwt () = Lwt_unix.sleep 0.01 in
  assert (!nthreads = 1);
  let c2 = connect server in
  lwt () = Lwt_unix.sleep 0.01 in
  assert (!nthreads = 2);
  let sl1 = 0.025 in
  let sl2 = 0.035 in
  lwt () = send c1 @@ `Req sl1 in
  lwt () = send c2 @@ `Req sl2 in
  lwt () = Lwt_unix.sleep 0.01 in 
  (* time left: 0.015±e, 0.025±e *)
  assert (!nreqs = 2);
  lwt () = Lwt_unix.sleep 0.01 in 
  (* time left: 0.005±e, 0.015±e *)
  assert (!nreqs = 2);
  lwt () = Lwt_unix.sleep 0.01 in 
  (* time left: none, 0.005±e *)
  assert (!nreqs = 1);
  lwt (rs1, tid1) = recv c1 in
  assert (rs1 = sl1);
  assert (!nreqs = 1);
  lwt (rs2, tid2) = recv c2 in
  assert (!nreqs = 0);
  assert (rs2 = sl2);
  assert (tid1 <> tid2);
  lwt () = send c1 `Stop in
  lwt () = Lwt_unix.sleep 0.01 in
  assert (!nthreads = 1);
  lwt () = send c2 `Stop in
  lwt () = Lwt_unix.sleep 0.01 in
  assert (!nthreads = 0);
  return_unit

let () = Lwt_main.run begin
  try_lwt
  (*- Printf.eprintf "\n\n\n\n\n\n\n\n%!"; -*)
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
      ; switch_ok
      ; switch_inner_close
      ; thread_test
      ]
  in
  print_endline "Lwt_comm tests passed ok.";
  return_unit
with e -> Printf.eprintf "Lwt_comm tests exception: %s\n%s\n%!"
  (Printexc.to_string e)
  (Printexc.get_backtrace ());
 return_unit
end
