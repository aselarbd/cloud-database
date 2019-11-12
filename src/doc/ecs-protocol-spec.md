# ECS protocol specification

## Messages and their format

Messages are extending the standard telnet protocol used for client-server communication.

Each message is to be responded with `ok` or `error`, which is not explicitly shown here.
In case of error, a series of messages (like node addition) is to be aborted.

### Locking

#### Write lock

* ECS -> Server: `write_lock`

#### Release a lock

* ECS -> Server: `release_lock` 

### Node management

#### register a new node

* Server -> ECS: `register`

#### Changing the successor node
     
* ECS -> Server: `next_addr <address> <port>`

#### Data transferral - range

* ECS -> Server: `transfer_range <address> <port>`

  *this sends everything between `next_addr`'s hash and the given address hash to the specified server*
* Server1 -> Server2: `ecs_put <encoded data items>` (for the server-to-server exchange initiated by `transfer_range`)

  There may be multiple `put` messages.
* Server2 -> ECS: `done`

#### Data transferral - all

* ECS -> Server: `transfer_all <address> <port>`

  *sends everything to the destination*
* Uses the same `ecs_put` etc. like range transfer


#### Node metadata announcement

* ECS -> all: `broadcast_new <address> <port>`
* ECS -> all: `broadcast_rem <address> <port>`

#### Removing a node

* Server -> ECS: `announce_shutdown`

### Monitoring

* ECS -> Server: `ping`


## Messages combined

### New node

The `ecs_put` may be split up to multiple messages.

```
    ECS     Server 1 (new)  Server 2             
     |           |             |                 
     |<----------|             |                 
     |  register |             |                 
     |           |             |                 
     |---------->|             |                 
     | next_addr |             |                 
     | of svr 3  |             |                 
     |           |             |                 
     |------------------------>|                 
     |       write_lock        |                 
     |           |             |                 
     |------------------------>|                 
     |   next_addr svr 1       |                 
     |           |             |                 
     |------------------------>|                 
     |     transfer_range      |                 
     |       s1 .. s3          |                 
     |           |             |                 
     |           |<------------|                 
     |           |   ecs_put   |                 
     |           |             |                 
     |           |<------------|                 
     |           |    done     |                 
     |<------------------------|                 
     |   done    |             |                 
     |           |             |                 
 broadcast       |             |                 
 _new s1         |             |                 
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
   |    announce    |                         |             * old ring:          
   |    _shutdown   |                         |             svr 2 - svr 1 - svr 3
   |                |                         |             new ring:            
   |--------------->|                         |             svr 2 - svr 3        
   |   write_lock   |                         |                                  
   |                |                         |                                  
   |----------------------------------------->|                                  
   |             next_addr svr 3*             |                                  
   |                |                         |                                  
   |--------------->|                         |                                  
   | transfer_all   |                         |                                  
   |      s_2       |------------------------>|                                  
   |                |       ecs_put           |                                  
   |                |                         |                                  
   |                |<------------------------|                                  
   |                |        done             |                                  
   |                |                         |                                  
   |<-----------------------------------------|                                  
   |                |       done              |                                  
 broadcast          |                         |                                  
 _rm s1             |                         |                                  
   |                |                         |                                  
   |--------------->|                         |                                  
   | release_lock   |                         |                                  
   |                X                         |                                  
   |                                          |                                  
   |                                          |                                  
```