# AMQP Package

AMQP binding implementing Client-Server and Publisher-Consumer paradigms

### Server Example
 
```Go
    package main
    
    import (
        "fmt"
        "github.com/djumanoff/amqp"
    )
    
    var cfg = amqp.Config{
        Host: "localhost",
        VirtualHost: "",
        User: "admin",
        Password: "admin",
        Port: 5672,
        LogLevel: 5,
    }
    
    var srvCfg = amqp.ServerConfig{
        ResponseX: "in.fanout",
        RequestX: "request",
    }
    
    func main() {
        fmt.Println("Start")
    
        sess := amqp.NewSession(cfg)
    
        if err := sess.Connect(); err != nil {
            fmt.Println(err)
            return
        }
        defer sess.Close()
    
        srv, err := sess.Server(srvCfg)
        if err != nil {
            fmt.Println(err)
            return
        }
    
        srv.Endpoint("request.get.test", func(d amqp.Message) *amqp.Message {
            fmt.Println("handler called")
            return &amqp.Message{
                Body: []byte("test"),
            }
        })
    
        if err := srv.Start(); err != nil {
            fmt.Println(err)
            return
        }
    
        fmt.Println("End")
    }
```

### Client Example 

```Go
    package main
        
    import (
    	"fmt"
    	"github.com/djumanoff/amqp"
    )
    
    var cfg = amqp.Config{
    	Host: "localhost",
    	VirtualHost: "",
    	User: "admin",
    	Password: "admin",
    	Port: 5672,
    	LogLevel: 5,
    }
    
    func main() {
    	fmt.Println("Start")
    
    	sess := amqp.NewSession(cfg)
    
    	if err := sess.Connect(); err != nil {
    		fmt.Println(err)
    		return
    	}
    	defer sess.Close()
    
    	var cltCfg = amqp.ClientConfig{
    		ResponseX: "response",
    		RequestX: "in.fanout",
    	}
    
    	clt, err := sess.Client(cltCfg)
    	if err != nil {
    		fmt.Println(err)
    		return
    	}
    
    	reply, err := clt.Call("request.get.test", amqp.Message{
    		Body: []byte("ping 1"),
    	})
    	if err != nil {
    		fmt.Println(err)
    		return
    	}
    
    	fmt.Println(reply)
    	fmt.Println("End")
    
    	ch := make(chan bool)
    
    	<-ch
    }
```
