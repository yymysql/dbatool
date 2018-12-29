/*
1. 删除redis各种类型的大key
2. 对于list类型是通过ltrim每次删除少量元素
3. 对于hash是通过hscan每次获取一定字段然后通过hdel删除
4. 对于set是通过sscan每次扫描结合中的500个元素，再使用srem命令删除
5. 对于sortset的删除和list相似，使用自带的zremrangebyrank每次删除指定元素
6. author,yayun,20181219
7. 对于redis 4.0以后无需这么麻烦
*/
package main

import (
	"flag"
	"fmt"
	"github.com/go-redis/redis"
	"os"
	"strconv"
	"strings"
)

var (
	help   bool
	host   string
	port   int
	auth   string
	bigkey string
)

func init() {
	flag.BoolVar(&help, "help", false, "this help")
	flag.StringVar(&host, "host", "127.0.0.1", "set redis `host`")
	flag.StringVar(&auth, "auth", " ", "set redis password `auth`")
	flag.StringVar(&bigkey, "bigkey", " ", "del redis `bigkey`")
	flag.IntVar(&port, "port", 6379, "set redis `port`")
	flag.Usage = usage
}

func main() {
	flag.Parse()

	if help {
		flag.Usage()
		os.Exit(0)
	}

	_, err := createClient(host, port, auth)
	if err != nil {
		fmt.Printf("conn redis is Error: %s\n", err)
		os.Exit(1)

	}

	client, _ := createClient(host, port, auth)

	statu, _ := client.Exists(bigkey).Result()
	if statu == 0 {
		fmt.Printf("this key %v does not exists...\n", bigkey)
		os.Exit(1)
	}

	valtype, err := client.Type(bigkey).Result()
	if err != nil {
		os.Exit(1)
	}

	if valtype == "string" {
		fmt.Printf("this key is %v type is string...\n", bigkey)
		os.Exit(1)
	}

	switch valtype {
	case "list":
		dellistkey(bigkey,*client)
	case "set":
		delsetkey(bigkey,*client)
	case "zset":
		delsortsetkey(bigkey,*client)
	case "hash":
		delhashkey(bigkey,*client)
	default:
		fmt.Printf("this reids key:%v is unknown type...\n", bigkey)

	}

}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage: [-host redis host] [-port reeis port] [-auth redis password]
Options:`)
	fmt.Println()
	flag.PrintDefaults()
}

func dellistkey(rediskey string, client redis.Client) {

	num, _ := client.LLen(bigkey).Result()
	for num > 0 {
		num = num - 1
		_, err := client.LTrim(bigkey, 0, -101).Result()
		if err != nil {
			fmt.Printf("del redis bigkey:%v is error", bigkey, err)
			os.Exit(1)
		}
	}
	fmt.Printf("this redis key:%v type is list del success\n", bigkey)

}

func delhashkey(rediskey string, client redis.Client) {
	var cursor uint64 = 0

	for {
		num, _ := client.HLen(bigkey).Result()
		if num == 0 {
			break
		}

		cursor = cursor + 1
		val, _, err := client.HScan(bigkey, cursor, "*", 100).Result()
		if err != nil {
			os.Exit(1)
		}

		for idx, value := range val {
			residue := idx % 2
			if residue == 0 {
				_, err := client.HDel(bigkey, value).Result()
				if err != nil {
					fmt.Printf("del redis hash key error.. %v", err)
					os.Exit(1)
				}

			}

		}
	}
	fmt.Printf("this redis key:%v type is hash del success\n", bigkey)

}

func delsetkey(rediskey string, client redis.Client) {
	var cursor uint64 = 0

	for {
		num, _ := client.SCard(bigkey).Result()
		if num == 0 {
			break
		}

		cursor = cursor + 1
		val, _, err := client.SScan(bigkey, cursor, "*", 100).Result()
		if err != nil {
			os.Exit(1)
		}

		for _, value := range val {
			_, err := client.SRem(bigkey, value).Result()
			if err != nil {
				fmt.Printf("del redis set key error.. %v", err)
				os.Exit(1)

			}

		}

	}
	fmt.Printf("this redis key:%v type is set del success\n", bigkey)
}

func delsortsetkey(rediskey string, client redis.Client) {

	for {
		num, _ := client.ZCard(bigkey).Result()
		if num == 0 {
			break
		}

		_, err := client.ZRemRangeByRank(bigkey, 0, 99).Result()
		if err != nil {
			fmt.Printf("del redis sortset key error.. %v", err)
			os.Exit(1)

		}

	}
	fmt.Printf("this redis key:%v type is sortset del success\n", bigkey)

}

func createClient(host string, port int, auth string) (*redis.Client, error) {
	rdsport := strconv.Itoa(port)
	conn := strings.Join([]string{host, rdsport}, ":")
	client := redis.NewClient(&redis.Options{
		Addr:     conn,
		Password: auth,
		DB:       0,
	})
	_, err := client.Ping().Result()
	return client, err
}

