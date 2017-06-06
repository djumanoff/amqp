# AMQP Package

AMQP binding implementing Client-Server and Publisher-Consumer paradigms

### Server Example
 
```Go
    package main
    
    import (
    	"fmt"
    	"bitbucket.org/darplay/domain/rmq"
    )
    
    var cfg = rmq.Config{
    	Host: "localhost",
    	VirtualHost: "",
    	User: "admin",
    	Password: "admin",
    	Port: 5672,
    	LogLevel: 5,
    }
    
    var srvCfg = rmq.ServerConfig{
    	ResponseX: "in.fanout",
    	RequestX: "request",
    }
    
    func main() {
    	fmt.Println("Start")
    
    	sess := rmq.NewSession(cfg)
    
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
    
    	srv.Endpoint("request.get.test", func(d rmq.Message) *rmq.Message {
    		fmt.Println("handler called")
    		return &rmq.Message{
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
    	"bitbucket.org/darplay/domain/rmq"
    )
    
    var cfg = rmq.Config{
    	Host: "localhost",
    	VirtualHost: "",
    	User: "admin",
    	Password: "admin",
    	Port: 5672,
    	LogLevel: 5,
    }
    
    func main() {
    	fmt.Println("Start")
    
    	sess := rmq.NewSession(cfg)
    
    	if err := sess.Connect(); err != nil {
    		fmt.Println(err)
    		return
    	}
    	defer sess.Close()
    
    	var cltCfg = rmq.ClientConfig{
    		ResponseX: "response",
    		RequestX: "in.fanout",
    	}
    
    	clt, err := sess.Client(cltCfg)
    	if err != nil {
    		fmt.Println(err)
    		return
    	}
    
    	reply, err := clt.Call("request.get.test", rmq.Message{
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
