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

type Notification struct {
    Id int
    Type string
    Sender int
    Origin int
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

func marshal_notification(id int, type_ string, sender int, origin int) []byte {
    text := Notification{
        Id:     id,
        Type:   type_,
        Sender:   sender,
        Origin:   origin,
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

func CheckError(err error) {
    if err  != nil {
        fmt.Println("Error: " , err)
    } 
}

func message_neighbours(g graph.Graph, node graph.Node, message Message) {
    
    //get all neighbors
    neighbors, _ := g.Neighbors(node.Id())
    fmt.Println("neigh", node.Id(), neighbors)

    //select random neighbor
    neighbor_ind := rand.Intn(len(neighbors))
    neighbor := neighbors[neighbor_ind]

    //resolve addresses
    LocalAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
    CheckError(err)
    
    ServerAddr,err := net.ResolveUDPAddr("udp", fmt.Sprintf("127.0.0.1:%d", neighbor.Port()))
    CheckError(err)
        
    
    if neighbor.Id() != message.Sender {
        MsgConn, err := net.DialUDP("udp", LocalAddr, ServerAddr)
        CheckError(err)
            
        if (err == nil) {
            fmt.Println(fmt.Sprintf("message from:%d | to:%d | %s", node.Id(), neighbor.Id(), "multicast"))
                    
            buf := marshal_message(message.Id, "multicast", node.Id(), message.Origin, message.Data)
            _,err_ := MsgConn.Write(buf)
            CheckError(err_)   
                
            buf = marshal_message(message.Id, "notification", node.Id(), message.Origin, message.Data)
            _,err_ = MsgConn.Write(buf)
            CheckError(err_)   
        }

        MsgConn.Close()                
    }

    //time.Sleep(time.Second * 1) 
}

func node_daemon(g graph.Graph, node graph.Node) {
    
    //initialize listening to port
    ServerAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", node.Port()))
    fmt.Println("server started at port:", node.Port())
    CheckError(err)
    buf := make([]byte, 1024)
    Conn, err := net.ListenUDP("udp", ServerAddr)
    CheckError(err)

    if (node.Id() > 0) {
        messages_map := map[int]Message{}

        //infinite loop
        for i:=0;;i+=1 {

            //read message
            n,addr,err := Conn.ReadFromUDP(buf)
            CheckError(err)

            //parse JSON message
            json_str := fmt.Sprintf("'%s'",string(buf[0:n]))
            fmt.Println(fmt.Sprintf("node %d received ", node.Id()), json_str, " from ",addr)
            message := parse_json(buf[0:n])

            fmt.Println(messages_map)

            //add message to map if it is new and multicast
            if _, ok := messages_map[message.Id]; !ok {
                if message.Type == "multicast" {
                    messages_map[message.Id] = message  
                }
            }

            //iterate over all messages and send to random neighbor
            for _, v := range messages_map { 
                message_neighbours(g, node, v)
            }
        }

    } else
    {            
        message_neighbours(g, node, Message{0, "multicast", 0, 0, "bla"})
        
        reply_got := map[int]bool{}
    
        for {
            n,addr,err := Conn.ReadFromUDP(buf)
            CheckError(err)

            json_str := fmt.Sprintf("'%s'",string(buf[0:n]))
            fmt.Println(fmt.Sprintf("node %d received ", node.Id()), json_str, " from ",addr)
            message := parse_json(buf[0:n])

            if message.Type == "notification" {
                reply_got[message.Sender] = true 
            }

            fmt.Println(reply_got)
            if len(reply_got) == len(g) - 1 {
                fmt.Println("FINISH")

            } 
        }
    }
}


func main() {
    const n = 5
    const basic_port = 10000
    net := graph.Generate(n, n-1, n, basic_port)
    for i := 0; i < n; i++ {
        cur_node, is_ok := net.GetNode(i)
        neigh, is_ok := net.Neighbors(cur_node.Id())
        fmt.Println(cur_node.Id(), cur_node.Port(), neigh)
        if is_ok {
            go node_daemon(net, cur_node)
        }       
    }

       
    time.Sleep(time.Second*100)
}