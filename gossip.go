package main

import (
    "./graph"
    "fmt"
    "net"
    "time"
    "encoding/json"
    "math/rand"
)

type Message struct {
    Id int
    Type string
    Sender int
    Origin int
    Data string
}

func marshal_message(id int, type_ string, sender int, origin int, data string) []byte {
    text := Message{
        Id:     id,
        Type:   type_,
        Sender:   sender,
        Origin:   origin,
        Data:   data,
    }
    b, err := json.Marshal(text)
    if err != nil {
        fmt.Println("error:", err)
    }
    return b
}

func parse_json (json_string []byte) Message {
    c := make(map[string]interface{})
    e := json.Unmarshal(json_string, &c)
    CheckError(e)

    msg_type := c["Type"].(string)
    id := int(c["Id"].(float64))
    origin := int(c["Origin"].(float64))
    sender := int(c["Sender"].(float64))
    
    if msg_type == "multicast" {
        return Message{id, msg_type, sender, origin, c["Data"].(string)}
    }

    return Message{id, msg_type, sender, origin, ""}
}

func CheckError(err error) bool{
    if err  != nil {
        fmt.Println("Error: " , err)
        return false;
    } 
    return true;
}

func message_neighbours(g graph.Graph, node graph.Node, message Message) bool{
    
    //get all neighbors
    neighbors, _ := g.Neighbors(node.Id())

    //select random neighbor
    neighbor_ind := rand.Intn(len(neighbors))
    for neighbors[neighbor_ind].Id() == message.Sender {
        neighbor_ind = rand.Intn(len(neighbors))
    }
    neighbor := neighbors[neighbor_ind]
    
    //resolve addresses
    LocalAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
    if (!CheckError(err)) {
        return false;
    }
    ServerAddr,err := net.ResolveUDPAddr("udp", fmt.Sprintf("127.0.0.1:%d", neighbor.Port()))
    if (!CheckError(err)) {
         return false;
    }    

    Conn, err := net.ListenUDP("udp", LocalAddr)
    CheckError(err)
    defer Conn.Close()

    //send to neighbor if it is different from sender
    if neighbor.Id() != message.Sender {
        
        if (!CheckError(err)) {
           return false;
        }    
            
        //send message
        buf := marshal_message(message.Id, message.Type, node.Id(), message.Origin, message.Data)
        _,err_ := Conn.WriteToUDP(buf, ServerAddr)
        CheckError(err_)   
        return true;            
    }

    return true;
}

func node_daemon(g graph.Graph, node graph.Node, stop *bool, current_time *int) {
    
    //initialize listening to port
    ServerAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("127.0.0.1:%d", node.Port()))
    CheckError(err)
    Conn, err := net.ListenUDP("udp", ServerAddr)
    CheckError(err)
    defer Conn.Close()
    buf := make([]byte, 1024)

    if (node.Id() > 0) {
        messages_map := map[int]Message{}

        for i:=0;*stop == false;i+=1 {

            //read message
            Conn.SetReadDeadline(time.Now().Add(time.Second))
            n,_,err := Conn.ReadFromUDP(buf)
            CheckError(err)
            
            if err == nil {
                //parse JSON message
                message := parse_json(buf[0:n])
                
                //add message to map if it is new
                if _, ok := messages_map[message.Id]; !ok {
                    messages_map[message.Id] = message 

                    //if it multicast, also add notification of it 
                    if message.Type == "multicast" {
                        new_id := message.Id + 100000 * node.Id()
                        messages_map[new_id] = Message{new_id, "notification", node.Id(), node.Id(), ""}
                    }
                }
            }

            //iterate over all messages and send to random neighbor
            for _, v := range messages_map { 
                message_neighbours(g, node, v)
            }

            time.Sleep(time.Second / 10)
        }

    } else //actions for root node
    {            

        reply_got := map[int]bool{}
        
        //wait until all servers start
        time.Sleep(time.Second) 

        for t:=0;;t+=1{
            
            //send to neighbors
            message_neighbours(g, node, Message{0, "multicast", 0, 0, "from root"})
            if *stop == false {
                *current_time += 1
            }

            //read message
            Conn.SetReadDeadline(time.Now().Add(time.Second))
            n,_,err := Conn.ReadFromUDP(buf)
            CheckError(err)

            if err == nil {
                //parse JSON message
                message := parse_json(buf[0:n])
                
                //mark that a node got initial message
                if message.Type == "notification" {
                    reply_got[message.Origin] = true 
                }

                fmt.Println("replies", len(reply_got), *current_time)
                if len(reply_got) == len(g) - 1 {
                    *stop = true;
                }
            }

            time.Sleep(time.Second / 10)
        }
    }
}


func main() {
    const n = 10
    const basic_port = 10000
    net := graph.Generate(n, 2, 10, basic_port)
    
    stop := false
    current_time := 0

    for i := 0; i < n; i++ {
        cur_node, is_ok := net.GetNode(i)
        neigh, is_ok := net.Neighbors(cur_node.Id())
        fmt.Println(cur_node.Id(), cur_node.Port(), neigh, is_ok)
        if is_ok {
            go node_daemon(net, cur_node, &stop, &current_time)
        }       
    }
    for stop == false { 
        time.Sleep(time.Second / 100)
    }
    fmt.Println("TOTAL TIME", current_time)
}