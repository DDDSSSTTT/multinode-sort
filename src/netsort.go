package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"sort"
	"strconv"
	"time"

	"gopkg.in/yaml.v2"
)

type Client struct {
	Conn net.Conn
	// Value int
	Text string
}

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

// Some global parameters
var TOTAL int = 4

func check(e error) {
	if e != nil {
		panic(e)
	}
}
func read_input(input_path string) (map[[3]int]string, [][3]int) {
	//Data input handling
	//Return a map with its key
	file, err := os.Open(input_path)
	check(err)
	fi, err := file.Stat()
	check(err)
	var val_key [3]int
	this_map := make(map[[3]int]string)
	b2 := make([]byte, 100)
	var i int64 = 0
	for ; i < fi.Size(); i = i + 100 {
		_, err := file.Seek(i, 0)
		check(err)
		file.Read(b2)
		val_key[0] = int(binary.BigEndian.Uint32(b2[:4]))
		val_key[1] = int(binary.BigEndian.Uint32(b2[4:8]))
		val_key[2] = int(binary.BigEndian.Uint16(b2[8:10]))
		val := string(b2[10:])
		this_map[val_key] = val
	}
	file.Close()

	keys := make([][3]int, 0, len(this_map))
	for k := range this_map {
		keys = append(keys, k)
	}
	return this_map, keys
}
func partition_add2ch(this_map map[[3]int]string, keys [][3]int, ch chan<- Client, conn_map map[int]net.Conn) {
	fmt.Println("Start Partitioning")
	//the mask of get first log2(TOTAL) bits
	var mask uint32 = 0
	for i := 1; i < TOTAL; i = i << 1 {
		mask += 1 << (31 - int(math.Log2(float64(i))))
	}
	this_line := make([]byte, 100)
	fmt.Println("Start adding items to channel")
	for _, k := range keys {
		server_idx := int(uint32(k[0])&mask) >> int(32-math.Log2(float64(TOTAL)))
		if conn_map[server_idx] != nil { //BE CAREFUL might Hide ERROR
			binary.BigEndian.PutUint32(this_line[0:4], uint32(k[0]))
			binary.BigEndian.PutUint32(this_line[4:8], uint32(k[1]))
			binary.BigEndian.PutUint16(this_line[8:10], uint16(k[2]))
			copy(this_line[10:100], []byte(this_map[k]))
			// fmt.Println("Package to Server:", strconv.Itoa(server_idx), "Correspondent_conn:", conn_map[server_idx])
			ch <- Client{conn_map[server_idx], string(this_line)}
			delete(this_map, k) //Note:very tricky here
			// fmt.Println("delete a line with key", k[0], k[1], k[2], " from the map")
		}

	}
	time.Sleep(1 * time.Second)
	for each_server_id := range conn_map {
		if conn_map[each_server_id] != nil {
			conn_map[each_server_id].Close()
		}
	}
}
func channelsend(ch <-chan Client) {

	for this := range ch {
		if this.Conn != nil {
			fmt.Println("Start Sending to ", this.Conn)
			conn := this.Conn
			conn.Write([]byte(this.Text))
		}

	}
}
func init_new_map(old_map map[[3]int]string, serverId int) map[[3]int]string {
	//Convert old map to new map
	var mask uint32 = 0
	for i := 1; i < TOTAL; i = i << 1 {
		mask += 1 << (31 - int(math.Log2(float64(i))))
	}
	// this_line := make([]byte, 100)
	new_map := make(map[[3]int]string, len(old_map))
	for k, v := range old_map {
		server_idx := int(uint32(k[0])&mask) >> int(32-math.Log2(float64(TOTAL)))
		if server_idx != serverId {
			//Do nothing as we need not to copy the sent item to new_map
		} else {
			new_map[k] = v
		}
	}
	return new_map
}

func dist_sort(this_map map[[3]int]string, output_path string) int {
	keys := make([][3]int, 0, len(this_map))
	for k := range this_map {
		keys = append(keys, k)
	}
	//Sorting and generate output
	sort.Slice(keys, func(i, j int) bool {
		if keys[i][0] != keys[j][0] {
			return keys[i][0] < keys[j][0]
		} else if keys[i][1] != keys[j][1] {
			return keys[i][1] < keys[j][1]
		} else if keys[i][2] != keys[j][2] {
			return keys[i][2] < keys[j][2]
		} else {
			return true
		}
	})
	fmt.Println("Start Writing the Output")

	file, err := os.Create(output_path)
	check(err)

	var this_line []byte
	this_line = make([]byte, 100)
	for _, k := range keys {
		binary.BigEndian.PutUint32(this_line[0:4], uint32(k[0]))
		binary.BigEndian.PutUint32(this_line[4:8], uint32(k[1]))
		binary.BigEndian.PutUint16(this_line[8:10], uint16(k[2]))
		// fmt.Println("a line with key", k[0], k[1], k[2])
		copy(this_line[10:100], []byte(this_map[k]))
		file.Write(this_line)
	}
	file.Sync()
	file.Close()
	return 0
}
func readServerConfigs(configPath string) ServerConfigs {
	f, err := ioutil.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	err = yaml.Unmarshal(f, &scs)

	return scs
}
func startaListener(id int, scs ServerConfigs) net.Listener {
	lis, err := net.Listen("tcp", scs.Servers[id].Host+":"+scs.Servers[id].Port)
	fmt.Println("listener established")
	check(err)
	return lis
}
func acceptParse(id int, l net.Listener, ch chan<- Client, conn_map map[int]net.Conn) {
	client_num := 0

	// client_map := make(map[net.Conn]string)
	for i := 0; i < 10; i++ {
		fmt.Println("Try to Accept after 1 seconds")
		time.Sleep(1 * time.Second)

		conn, _ := l.Accept()
		if conn != nil {
			client_num += 1
			this_id := make([]byte, 10)
			this_len, _ := conn.Read(this_id)
			fmt.Println("server", string(this_id), "connected")
			//Modify the conn map (hopefully)
			this_i, _ := strconv.Atoi(string(this_id[:this_len]))
			conn_map[this_i] = conn
			fmt.Println(string(this_id[:this_len]))
			//IMPORTANT: add stuff to the channel
			//ch <- Client{conn, "Hello from" + strconv.Itoa(id)}
			//client_map[conn] = "Hello from" + strconv.Itoa(id)
		}
		if client_num >= TOTAL-1 && conn != nil {
			fmt.Println("All servers connected, exit the Accept process")
			// Add the code of sending (to channel)

			break
		}

	}

}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	fmt.Println("My server Id:", serverId)

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	fmt.Println("Got the following server configs:", scs)
	/*
		Implement Distributed Sort
	*/
	ch := make(chan Client)
	// Add the code of preprocessing here
	old_map, old_key := read_input(os.Args[2])
	new_map := init_new_map(old_map, serverId)
	lis := startaListener(serverId, scs)

	var conn net.Conn = nil
	conn_map := make(map[int]net.Conn)
	for connectserverId := 0; connectserverId < 4; connectserverId++ {
		if connectserverId == serverId {
			//We decide to move channel inside to listenforData
			go channelsend(ch)
			go acceptParse(serverId, lis, ch, conn_map)
			time.Sleep(4 * time.Second)
			for k := range conn_map {
				fmt.Println(k, (conn_map[k]))
			}
			partition_add2ch(old_map, old_key, ch, conn_map)
			lis.Close()
			continue
		}
		fmt.Println("Connect to server", connectserverId)
		for i := 0; i < 6; i++ {
			time.Sleep(500 * time.Millisecond) //Redial
			fmt.Println("Connect Attempt:", i)
			conn, err = net.Dial("tcp", scs.Servers[connectserverId].Host+":"+scs.Servers[connectserverId].Port)
			if conn != nil {
				fmt.Println("Connection established", conn)
				conn.Write([]byte(strconv.Itoa(serverId)))
				defer conn.Close()
				break

			}

		}
		buffer := make([]byte, 100)
		if connectserverId == serverId {

		} else {
			fmt.Println("Currently Reading")
			// Add the code of receiving
			for {
				n, err := conn.Read(buffer)
				if err != nil {
					if err != io.EOF {
						fmt.Println("read error:", err)
					}
					break
				}
				// fmt.Println("got", n, "bytes.")
				// fmt.Println("Message:", string(buffer[:n]))
				var buf_key [3]int
				buf_key[0] = int(binary.BigEndian.Uint32(buffer[:4]))
				buf_key[1] = int(binary.BigEndian.Uint32(buffer[4:8]))
				buf_key[2] = int(binary.BigEndian.Uint16(buffer[8:10]))
				buf_val := string(buffer[10:n])
				new_map[buf_key] = buf_val
			}
			// bytes, err := conn.Read(buffer)
			// check(err)
			// fmt.Println("Message sent by somebody ->" + string(buffer[:bytes]))
		}

	}
	//Add the code of after-processing & output
	dist_sort(new_map, os.Args[3])
	// ch := make(chan Client)
}
