depath 0                                       [ 02, 3f, 55, e6, ..., ff ] node_256_t (256 bytes) 
                                                 /                   
                                                /                        
depath 1                                 [ 33, 44, ..., 2f ] node_48_t (48 bytes max)
                                                |   
                                                |            
depath 2                                 [ ..., 11, ... ] node_16_t (16 bytes max)
                                                |    
                                                |
depath 3                                 [ ..., af, ... ] node_4_t (4 byte max)
                                                |
...                                            ...               

depath 32                                [ ..., 4c, ... ] 
                                                |    
                                                |
leafs                                         value