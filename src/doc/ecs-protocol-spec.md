# ECS protocol specification

## Messages and their format

Messages are extending the standard telnet protocol used for client-server communication.

Each message is to be responded with `ok` or `error`

### Locking

#### Write lock

* ECS -> Server: `write_lock`

#### Release a lock

* ECS -> Server: `release_lock` 

### Node management

#### register a new node
     
s
* ECS -> Server: `next_addr <address> <port>`
* ECS -> Server: `transfer_range <address> <port>`

  *this sends everything between `next_addr`'s hash and the given address hash to the specified server*
* Server1 -> Server2: `ecs_put <encoded data items>` (for the server-to-server exchange initiated by `transfer_range`)
* Server2 -> ECS: `done`
* ECS -> all: `broadcast_node <address> <port>`

## Messages combined

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
 metadata        |             |                 
 update          |             |                 
     |           |             |                 
     |------------------------>|                 
     |      release_lock       |                 
     |           |             |                 
     |           |             |                 
```