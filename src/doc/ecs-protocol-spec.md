# ECS protocol specification

## Messages and their format

Messages are extending the standard telnet protocol used for client-server communication.

### Locking

#### Write lock

* ECS -> Server: `write_lock`

#### Release a lock

* ECS -> Server: `release_lock` 

### Node management

#### register a new node

* Server -> ECS: `register <ip:port>`

#### Data transferral

* Server1 -> Server2: `put <key> <value>` (for each item)
* Server2 -> ECS: `done`

#### Node metadata announcement

* ECS -> new Server: `keyrange <from>,<to>,<ip:port>;<from>,<to>,<ip:port>;...`

  The keyrange format is identical to the message used for clients.

  This sends the ordered ring structure to bootstrap a new server and update the keyranges of existing servers.

#### Removing a node

* Server -> ECS: `announce_shutdown <ip:port>`

### Monitoring

* ECS -> Server: `ping`


## Messages combined

### New node

```
    ECS     Server 1 (new)  Server 2             
     |           |             |                 
     |<----------|             |                 
     |  register |             |                 
     |           |             |                 
     |---------->|             |                 
     | keyrange  |             |                 
     |           |             |                 
     |------------------------>|                 
     |       write_lock        |                 
     |           |             |                 
     |------------------------>|                 
     |         keyrange        |    server notices that
     |           |             |    the predecessor changed
     |           |             |                 
     |           |<------------|                 
     |           |     put     |                 
     |           |<------------|                 
     |           |     put     |   
     .           .             .
     .           .             .
     .           .             .
     |           |             |                             
     |<------------------------|                 
     |          ok             |                 
     |           |             |                 
 broadcast       |             |                 
 keyrange        |             |                 
     |           |             |                 
     |           |             |                 
     |------------------------>|                 
     |      release_lock       |                 
     |           |             |                 
     |           |             |                 
```

### Node removal

```
  ECS    Server 1 (about to leave)         Server 2                              
   |                |                         |                                  
   |<---------------|                         |                                  
   |    announce    |                         |             old ring:          
   |    _shutdown   |                         |             svr 2 - svr 1 - svr 3
   |                |                         |             new ring:            
   |--------------->|                         |             svr 2 - svr 3        
   |   write_lock   |                         |                                  
   |                |                         |                                  
   |----------------------------------------->|                                 
   |             keyrange                     |                                  
   |                |                         |                                  
   |--------------->|                         |                                  
   |   keyrange*    |                         |            * server notifies that it isn't included
   |                |------------------------>|              in keyrange anymore and therefore starts
   |                |           put           |              handoff             
   |                |                         |                              
   |                |------------------------>|              
   |                |           put           |                   
   |                |                         |
   .                .                         .
   .                .                         .
   .                .                         .                                  
   |                |                         |                                  
   |<-----------------------------------------|                                  
   |                |       done              |                                  
 replace            |                         |                                  
   |                |                         |                                  
   |                |                         |                                  
   |--------------->|                         |                                  
   | release_lock   |                         |                                  
   |                X                         |                                  
   |                                          |                                  
   |                                          |                                  
```