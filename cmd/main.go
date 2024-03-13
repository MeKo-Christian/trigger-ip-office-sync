package main

import (
	"fmt"
	"log"
	"os"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type Config struct {
	DbPort     string `envconfig:"DB_PORT"`
	DbUser     string `envconfig:"DB_USER"`
	DbPassword string `envconfig:"DB_PASS"`
	DbHost     string `envconfig:"DB_HOST"`
	DbName     string `envconfig:"DB_NAME"`
}

// Read the evironment variables into the config struct
func readEnv(cfg *Config) error {

	// load from .env file (if available
	err := godotenv.Load()
	if err != nil {
		log.Println("Error loading .env file")
	}

	const prefix = "TRIGGER_IPO_SYNC"
	if err := envconfig.Process(prefix, cfg); err != nil {
		return err
	}
	if cfg.DbPort == "" {
		return errors.Errorf("environment variable not set: DB_PORT")
	}
	if cfg.DbUser == "" {
		return errors.Errorf("environment variable not set: DB_USER")
	}
	if cfg.DbPassword == "" {
		return errors.Errorf("environment variable not set: DB_PASS")
	}
	if cfg.DbHost == "" {
		return errors.Errorf("environment variable not set: DB_HOST")
	}
	if cfg.DbPort == "" {
		return errors.Errorf("environment variable not set: DB_PORT")
	}
	if cfg.DbName == "" {
		return errors.Errorf("environment variable not set: DB_NAME")
	}
	return nil
}

func initDbConnection(cfg *Config) (*sqlx.DB, error) {
	var connectionString = fmt.Sprintf("server=%s;user id=%s;password=%s;port=%s;database=%s;",
		cfg.DbHost, cfg.DbUser, cfg.DbPassword, cfg.DbPort, cfg.DbName)

	db, err := sqlx.Open("sqlserver", connectionString)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func main() {
	var cfg Config

	logger, _ := zap.NewProduction()
	defer logger.Sync()
	sugar := logger.Sugar()

	// read variables from environment
	if err := readEnv(&cfg); err != nil {
		sugar.Fatalw("reading env variables failed", "error", err)
		os.Exit(1)
	}

	// check if DB connection posible
	db, err := initDbConnection(&cfg)
	if err != nil {
		sugar.Fatalw("DB connection failed", "error", err)
	}
	defer db.Close()

	// read the AK ID from environment
	var akID = os.Getenv("TRIGGER_IPO_SYNC_AK_NUM")
	if akID == "" {
		// if not found, ask the user
		fmt.Print("Bitte die Arbeitskartennummer eingeben: ")
		fmt.Scanln(&akID)
	}

	// select the Arbeitskarte from the DB
	selectQuery := fmt.Sprintf("SELECT TOP 1 WERKSTÜCK_ID FROM dbo.Arbeitskarte WHERE Nummer = '%s'", akID)
	rows, err := db.Query(selectQuery)
	if err != nil {
		sugar.Fatalw("Error selecting Arbeitskarte", "error", err)
	}

	// get the Werkstück IDs from the Arbeitskarte (field "Werkstück_ID")
	var workpieceID int
	if rows.Next() {
		err = rows.Scan(&workpieceID)
		if err != nil {
			sugar.Fatalw("Error scanning Arbeitskarte", "error", err)
		}
	} else {
		sugar.Fatalw("Arbeitskarte nicht gefunden", "error", err)
	}

	defer rows.Close()

	// update the Werkstück
	updateQuery := fmt.Sprintf("UPDATE dbo.Werkstück SET MekoRPUpdateCount = MekoRPUpdateCount + 1 WHERE id = %d", workpieceID)
	result, err := db.Exec(updateQuery)
	if err != nil {
		sugar.Fatalw("Error updating Werkstück", "error", err)
	}
	if count, _ := result.RowsAffected(); count == 0 {
		sugar.Fatalw("Werkstück nicht gefunden", "error", err)
	}

	// update the Arbeitskarte
	updateQuery = fmt.Sprintf("UPDATE dbo.Arbeitskarte SET MekoRPUpdateCount = MekoRPUpdateCount + 1 WHERE Nummer = '%s'", akID)
	result, err = db.Exec(updateQuery)
	if err != nil {
		sugar.Fatalw("Error updating Arbeitskarte", "error", err)
	}
	if count, _ := result.RowsAffected(); count == 0 {
		sugar.Fatalw("Arbeitskarte nicht gefunden", "error", err)
	}

	// update the Produktionsdaten
	updateQuery = fmt.Sprintf("UPDATE dbo.Produktionsdaten SET MekoRPUpdateCount = MekoRPUpdateCount + 1 WHERE Nummer = '%s'", akID)
	result, err = db.Exec(updateQuery)
	if err != nil {
		sugar.Fatalw("Error updating Produktionsdaten", "error", err)
	}
	if count, _ := result.RowsAffected(); count == 0 {
		sugar.Fatalw("Arbeitskarte in Produktionsdaten nicht gefunden", "error", err)
	}

	log.Println("Update process completed successfully.")
}
