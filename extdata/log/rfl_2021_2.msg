Error: object 'train_ref_mu' not found
 
Traceback: 
 [1] "3: evalVis(expr)"                                                        
 [2] "2: capture.output(traceback(5, max.lines = 1000))"                       
 [3] "1: (function () "                                                        
 [4] "   {"                                                                    
 [5] "       er <- geterrmessage()"                                            
 [6] "       tb <- capture.output(traceback(5, max.lines = 1000))"             
 [7] "       log_print(er, hide_notes = TRUE, blank_after = FALSE)"            
 [8] "       log_print(\"Traceback:\", hide_notes = TRUE, blank_after = FALSE)"
 [9] "       log_print(tb)"                                                    
[10] "       log_quiet(er, msg = TRUE, blank_after = FALSE)"                   
[11] "       log_quiet(\"Traceback:\", msg = TRUE, blank_after = FALSE)"       
[12] "       log_quiet(tb, msg = TRUE)"                                        
[13] "   })()"                                                                 

