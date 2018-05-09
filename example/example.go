package main

import (
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/nulx8874/awesome-id"
)

var idWorkerMap = make(map[int]*awesome_id.IdWorker)

func main() {
	r := gin.Default()

	// Ping test
	r.GET("/ping", func(c *gin.Context) {
		c.String(200, "pong")
	})

	// Get ID
	r.GET("/worker/:id", func(c *gin.Context) {
		id, _ := strconv.Atoi(c.Params.ByName("id"))
		value, ok := idWorkerMap[id]
		if ok {
			nextId, _ := value.GetId()
			c.JSON(200, gin.H{"id": nextId})
		} else {
			idWorker, err := awesome_id.NewIdWorker(int64(id), 0)
			if err == nil {
				nid, _ := idWorker.GetId()
				idWorkerMap[id] = idWorker
				c.JSON(200, gin.H{"id": nid})
			} else {
				c.String(200, err.Error())
			}
		}
	})

	r.GET("/worker", func(c *gin.Context) {
		c.JSON(200, idWorkerMap)
	})

	// Listen and Server in 0.0.0.0:8080
	r.Run(":8080")
}
