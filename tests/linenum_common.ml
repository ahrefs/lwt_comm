module C = Lwt_comm

let linenum_unix_serverfunc : (_, _, [`Connect | `Bidi]) C.unix_func =
  C.unix_func_of_maps
    Lwt_io.read_line
    Lwt_io.write_line
