package main

import (
	"log"

	"github.com/zeerodex/goot/cmd"
	"github.com/zeerodex/goot/internal/config"
	"github.com/zeerodex/goot/internal/database"
	"github.com/zeerodex/goot/internal/repositories"
	"github.com/zeerodex/goot/internal/services"
)

func main() {
	cfg, err := config.LoadConfig("./")
	if err != nil {
		log.Fatalf("Unable to load config: %v", err)
	}

	db, err := database.InitDB()
	if err != nil {
		log.Fatalf("Unable to init database: %v", err)
	}
	defer db.Close()

	service, err := services.NewTaskService(repositories.NewTaskRepository(db), cfg)
	if err != nil {
		log.Fatalf("Unable to initialize service: %v", err)
	}
	defer service.WP().Stop()

	cmd.Execute(service, cfg)
}
