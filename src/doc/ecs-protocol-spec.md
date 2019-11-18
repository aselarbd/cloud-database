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
     
* ECS -> Server: `next_addr <ip:port>`

#### Data transferral

* ECS -> Server: `transfer_range <start as ip:port> <end as ip:port> <recipient as ip:port>`
* Server1 -> Server2: `ecs_put <encoded data items>` (for the server-to-server exchange initiated by `transfer_range`)

  There may be multiple `put` messages.
* Server2 -> ECS: `done`

#### Node metadata announcement

* ECS -> all: `broadcast_new <ip:port>`
* ECS -> all: `broadcast_rem <ip:port>`
* ECS -> new Server: `keyrange <from>,<to>,<ip:port>;<from>,<to>,<ip:port>;...`

  The keyrange format is identical to the message used for clients. Other ECS messages usually do not
  include hashes as they can be re-computed easily.

  This sends the ordered ring structure to bootstrap a new server

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
     | keyrange  |             |                 
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
   | transfer_range |                         |                                  
   |    s_1 .. s_3  |------------------------>|                                  
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