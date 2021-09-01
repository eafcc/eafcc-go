package main

// They are in a separate file because of a peculiarity of how Cgo generates and compiles C code - more details on the Wiki. The reason I'm not using the static inline trick for these functions is that we have to take their address.
// https://github.com/golang/go/wiki/cgo#export-and-definition-in-preamble

// void update_cb_go(void *user_data);
// void update_cb_c(void *user_data){
//     update_cb_go(user_data);
// }
import "C"