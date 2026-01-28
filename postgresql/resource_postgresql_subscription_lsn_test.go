package postgresql

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccPostgresqlSubscription_LSNPositioning(t *testing.T) {
	skipIfNotAcc(t)
	testSuperuserPreCheck(t)

	dbSuffixPub, teardownPub := setupTestDatabase(t, true, true)
	dbSuffixSub, teardownSub := setupTestDatabase(t, true, true)

	// Ensure clean replication origin state - must run BEFORE database teardown
	defer cleanupReplicationOrigins(t)
	defer teardownPub()
	defer teardownSub()

	dbNamePub, _ := getTestDBNames(dbSuffixPub)
	dbNameSub, _ := getTestDBNames(dbSuffixSub)

	// Create test schemas in both databases
	schemas := []string{"pub_schema"}
	createTestSchemas(t, dbSuffixPub, schemas, "")
	createTestSchemas(t, dbSuffixSub, schemas, "")

	// Create source table in publisher & sunbscriber databases
	createTestTableForReplication(t, dbSuffixPub)
	createTestTableForReplication(t, dbSuffixSub)

	// Use unique subscription name to avoid OID conflicts, create the replication slot
	slotName := fmt.Sprintf("test_slot_%s", dbSuffixSub)
	createPublicationAndReplicationSlotWithName(t, dbSuffixPub, slotName)

	// Get connection info for publisher
	pubConninfo := getConnInfo(t, dbNamePub)

	// Insert first row before creating subscription & capture LSN right after(target LSN for positioning)
	insertRow(t, dbSuffixPub, "row_before_lsn")
	capturedLSN := getCurrentLSN(t, dbSuffixPub)

	// Run the LSN positioning test with the captured LSN
	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			testCheckCompatibleVersion(t, featurePublication)
			testSuperuserPreCheck(t)
		},
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckPostgresqlSubscriptionDestroy,
		Steps: []resource.TestStep{
			{
				// Step 1: Create disabled subscription first (start_lsn not allowed during creation)
				Config: generateSubscriptionConfig(dbNameSub, pubConninfo, false, "null", slotName),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlSubscriptionExistsWithStreaming("postgresql_subscription.test_lsn", false),
					resource.TestCheckResourceAttr("postgresql_subscription.test_lsn", "enabled", "false"),
					testAccCheckSubscriptionEnabled("postgresql_subscription.test_lsn", false),
					testCheckReplicationOriginExists(dbNameSub, slotName),
				),
			},
			{
				// Step 2: Enable subscription with captured LSN (this is where LSN positioning happens)
				PreConfig: func() {
					// Insert second row BEFORE enabling subscription
					insertRow(t, dbSuffixPub, "row_after_lsn")
				},
				Config: generateSubscriptionConfig(dbNameSub, pubConninfo, true, fmt.Sprintf("\"%s\"", capturedLSN), slotName),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlSubscriptionExistsWithStreaming("postgresql_subscription.test_lsn", true),
					resource.TestCheckResourceAttr("postgresql_subscription.test_lsn", "enabled", "true"),
					testAccCheckSubscriptionEnabled("postgresql_subscription.test_lsn", true),
					func(s *terraform.State) error {
						// Check that LSN positioning worked correctly with retry logic
						return testCheckLSNReplication(dbNameSub)(s)
					},
				),
			},
		},
	})
}

func TestAccPostgresqlSubscription_WithoutLSNPositioning(t *testing.T) {
	skipIfNotAcc(t)
	testSuperuserPreCheck(t)

	dbSuffixPub, teardownPub := setupTestDatabase(t, true, true)
	dbSuffixSub, teardownSub := setupTestDatabase(t, true, true)

	// Ensure clean replication origin state - must run BEFORE database teardown
	defer cleanupReplicationOrigins(t)
	defer teardownPub()
	defer teardownSub()

	dbNamePub, _ := getTestDBNames(dbSuffixPub)
	dbNameSub, _ := getTestDBNames(dbSuffixSub)

	// Create test schemas in both databases
	schemas := []string{"pub_schema"}
	createTestSchemas(t, dbSuffixPub, schemas, "")
	createTestSchemas(t, dbSuffixSub, schemas, "")

	// Create source table in publisher & subscriber databases
	createTestTableForReplication(t, dbSuffixPub)
	createTestTableForReplication(t, dbSuffixSub)

	// Use unique subscription name to avoid OID conflicts, create the replication slot
	slotName := fmt.Sprintf("test_slot_%s", dbSuffixSub)
	createPublicationAndReplicationSlotWithName(t, dbSuffixPub, slotName)

	// Get connection info for publisher
	pubConninfo := getConnInfo(t, dbNamePub)

	// Insert first row before creating subscription (no LSN capturing needed)
	insertRow(t, dbSuffixPub, "row_before_lsn")

	// Run the test without LSN positioning
	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			testCheckCompatibleVersion(t, featurePublication)
			testSuperuserPreCheck(t)
		},
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckPostgresqlSubscriptionDestroy,
		Steps: []resource.TestStep{
			{
				// Step 1: Create disabled subscription first
				Config: generateSubscriptionConfig(dbNameSub, pubConninfo, false, "null", slotName),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlSubscriptionExistsWithStreaming("postgresql_subscription.test_lsn", false),
					resource.TestCheckResourceAttr("postgresql_subscription.test_lsn", "enabled", "false"),
					testAccCheckSubscriptionEnabled("postgresql_subscription.test_lsn", false),
					testCheckReplicationOriginExists(dbNameSub, slotName),
				),
			},
			{
				// Step 2: Enable subscription without LSN positioning
				PreConfig: func() {
					// Insert second row BEFORE enabling subscription
					insertRow(t, dbSuffixPub, "row_after_lsn")
				},
				Config: generateSubscriptionConfig(dbNameSub, pubConninfo, true, "null", slotName),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlSubscriptionExistsWithStreaming("postgresql_subscription.test_lsn", true),
					resource.TestCheckResourceAttr("postgresql_subscription.test_lsn", "enabled", "true"),
					testAccCheckSubscriptionEnabled("postgresql_subscription.test_lsn", true),
					func(s *terraform.State) error {
						// Check that both rows are replicated
						return testCheckFullReplication(dbNameSub)(s)
					},
				),
			},
		},
	})
}

// createTestTableForReplication creates the source table in database
func createTestTableForReplication(t *testing.T, dbSuffix string) {
	config := getTestConfig(t)
	dbName, _ := getTestDBNames(dbSuffix)

	db, err := sql.Open("postgres", config.connStr(dbName))
	if err != nil {
		t.Fatalf("could not connect to database: %v", err)
	}
	defer db.Close()

	// Create source table in database
	_, err = db.Exec(`
		CREATE TABLE pub_schema.test_table (
			id SERIAL PRIMARY KEY,
			data TEXT,
			created_at TIMESTAMP DEFAULT NOW()
		);
	`)
	if err != nil {
		t.Fatalf("could not create source table: %v", err)
	}
}

// createPublicationOnly creates publication via SQL in setup phase
func createPublicationAndReplicationSlotWithName(t *testing.T, dbSuffixPub string, slotName string) {
	config := getTestConfig(t)
	dbNamePub, _ := getTestDBNames(dbSuffixPub)

	db, err := sql.Open("postgres", config.connStr(dbNamePub))
	if err != nil {
		t.Fatalf("could not connect to publisher database: %v", err)
	}
	defer db.Close()

	// Create publication for the test table
	_, err = db.Exec(`CREATE PUBLICATION test_pub FOR TABLE pub_schema.test_table;`)
	if err != nil {
		t.Fatalf("could not create publication: %v", err)
	}

	// Create custom replication slot
	_, err = db.Exec(fmt.Sprintf("SELECT pg_create_logical_replication_slot('%s', 'pgoutput');", slotName))
	if err != nil {
		t.Fatalf("could not create replication slot %s: %v", slotName, err)
	}
}

// generateSubscriptionConfig generates config with only subscription (publication and slot created via SQL)
func generateSubscriptionConfig(dbNameSub, pubConninfo string, enabled bool, startLSN string, slotName string) string {
	return fmt.Sprintf(`
resource "postgresql_subscription" "test_lsn" {
	name         = "%s"
	database     = "%s"
	conninfo     = "%s"
	publications = ["test_pub"]
	enabled      = %t
	connect 	 = true
	create_slot  = false
	copy_data    = false
	start_lsn    = %s
}
`, slotName, dbNameSub, pubConninfo, enabled, startLSN)
}

// insertRow inserts a row with the given value
func insertRow(t *testing.T, dbSuffixPub string, value string) {
	config := getTestConfig(t)
	dbNamePub, _ := getTestDBNames(dbSuffixPub)

	db, err := sql.Open("postgres", config.connStr(dbNamePub))
	if err != nil {
		t.Fatalf("could not connect to publisher database: %v", err)
	}
	defer db.Close()

	// Insert second row (should be replicated)
	_, err = db.Exec("INSERT INTO pub_schema.test_table (data) VALUES ($1);", value)
	if err != nil {
		t.Fatalf("could not insert second row: %v", err)
	}
}

// getCurrentLSN captures the current WAL LSN without inserting data
func getCurrentLSN(t *testing.T, dbSuffixPub string) string {
	config := getTestConfig(t)
	dbNamePub, _ := getTestDBNames(dbSuffixPub)

	db, err := sql.Open("postgres", config.connStr(dbNamePub))
	if err != nil {
		t.Fatalf("could not connect to publisher database: %v", err)
	}
	defer db.Close()

	// Get current LSN
	var capturedLSN string
	err = db.QueryRow("SELECT pg_current_wal_lsn();").Scan(&capturedLSN)
	if err != nil {
		t.Fatalf("could not get current LSN: %v", err)
	}
	return capturedLSN
}

// testCheckLSNReplication verifies that LSN positioning worked correctly
func testCheckLSNReplication(dbNameSub string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		config := getTestConfig(nil)
		db, err := sql.Open("postgres", config.connStr(dbNameSub))
		if err != nil {
			return fmt.Errorf("could not connect to subscriber database: %v", err)
		}
		defer db.Close()

		// Debug: Check if the table exists in subscriber
		var tableExists bool
		err = db.QueryRow("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'pub_schema' AND table_name = 'test_table');").Scan(&tableExists)
		if err != nil {
			return fmt.Errorf("could not check if table exists: %v", err)
		}
		if !tableExists {
			return fmt.Errorf("table pub_schema.test_table does not exist in subscriber database")
		}

		var count int
		// First check how many total rows exist
		err = db.QueryRow("SELECT COUNT(*) FROM pub_schema.test_table;").Scan(&count)
		if err != nil {
			return fmt.Errorf("could not count total rows: %v", err)
		}

		if count > 0 {
			// Check for the expected row pattern
			err = db.QueryRow("SELECT COUNT(*) FROM pub_schema.test_table WHERE data = 'row_before_lsn';").Scan(&count)
			if err != nil {
				return fmt.Errorf("could not count row_before_lsn: %v", err)
			}
			if count != 0 {
				return fmt.Errorf("expected 0 rows with 'row_before_lsn', got %d", count)
			}

			err = db.QueryRow("SELECT COUNT(*) FROM pub_schema.test_table WHERE data = 'row_after_lsn';").Scan(&count)
			if err != nil {
				return fmt.Errorf("could not count row_after_lsn: %v", err)
			}
			if count == 1 {
				return nil // Success!
			}
			if count != 1 {
				return fmt.Errorf("expected 1 row with 'row_after_lsn', got %d", count)
			}
		}

		// If no data yet, wait and retry
		fmt.Printf("[DEBUG] No replicated data in subscriber. Checking publisher data...\n")

		// Final debug if still no data
		dbNamePub := strings.Replace(dbNameSub, "_sub", "_pub", 1)
		pubDb, err := sql.Open("postgres", config.connStr(dbNamePub))
		if err == nil {
			defer pubDb.Close()
			var pubRowCount int
			pubDb.QueryRow("SELECT COUNT(*) FROM pub_schema.test_table").Scan(&pubRowCount)
			fmt.Printf("[DEBUG] Total rows in publisher: %d\n", pubRowCount)

			rows, err := pubDb.Query("SELECT data FROM pub_schema.test_table ORDER BY id")
			if err == nil {
				defer rows.Close()
				var allData []string
				for rows.Next() {
					var data string
					rows.Scan(&data)
					allData = append(allData, data)
				}
				fmt.Printf("[DEBUG] Publisher data: %v\n", allData)
			}
		}
		return fmt.Errorf("no rows replicated - replication may not be working")
	}
}

// testCheckFullReplication verifies that both rows are replicated (no LSN positioning)
func testCheckFullReplication(dbNameSub string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		config := getTestConfig(nil)
		db, err := sql.Open("postgres", config.connStr(dbNameSub))
		if err != nil {
			return fmt.Errorf("could not connect to subscriber database: %v", err)
		}
		defer db.Close()

		// Debug: Check if the table exists in subscriber
		var tableExists bool
		err = db.QueryRow("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'pub_schema' AND table_name = 'test_table');").Scan(&tableExists)
		if err != nil {
			return fmt.Errorf("could not check if table exists: %v", err)
		}
		if !tableExists {
			return fmt.Errorf("table pub_schema.test_table does not exist in subscriber database")
		}

		var count int
		// First check how many total rows exist
		err = db.QueryRow("SELECT COUNT(*) FROM pub_schema.test_table;").Scan(&count)
		if err != nil {
			return fmt.Errorf("could not count total rows: %v", err)
		}

		if count > 0 {
			// Check that both rows are replicated
			err = db.QueryRow("SELECT COUNT(*) FROM pub_schema.test_table WHERE data = 'row_before_lsn';").Scan(&count)
			if err != nil {
				return fmt.Errorf("could not count row_before_lsn: %v", err)
			}
			if count != 1 {
				return fmt.Errorf("expected 1 row with 'row_before_lsn', got %d", count)
			}

			err = db.QueryRow("SELECT COUNT(*) FROM pub_schema.test_table WHERE data = 'row_after_lsn';").Scan(&count)
			if err != nil {
				return fmt.Errorf("could not count row_after_lsn: %v", err)
			}
			if count == 1 {
				return nil // Success!
			}
			if count != 1 {
				return fmt.Errorf("expected 1 row with 'row_after_lsn', got %d", count)
			}
		}

		// If no data yet, provide debug info
		fmt.Printf("[DEBUG] No replicated data in subscriber. Checking publisher data...\n")

		// Final debug if still no data
		dbNamePub := strings.Replace(dbNameSub, "_sub", "_pub", 1)
		pubDb, err := sql.Open("postgres", config.connStr(dbNamePub))
		if err == nil {
			defer pubDb.Close()
			var pubRowCount int
			pubDb.QueryRow("SELECT COUNT(*) FROM pub_schema.test_table").Scan(&pubRowCount)
			fmt.Printf("[DEBUG] Total rows in publisher: %d\n", pubRowCount)

			rows, err := pubDb.Query("SELECT data FROM pub_schema.test_table ORDER BY id")
			if err == nil {
				defer rows.Close()
				var allData []string
				for rows.Next() {
					var data string
					rows.Scan(&data)
					allData = append(allData, data)
				}
				fmt.Printf("[DEBUG] Publisher data: %v\n", allData)
			}
		}
		return fmt.Errorf("no rows replicated - replication may not be working")
	}
}

// testAccCheckSubscriptionEnabled checks subscription enabled state
func testAccCheckSubscriptionEnabled(resourceName string, expectedEnabled bool) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[resourceName]
		if !ok {
			return fmt.Errorf("resource not found: %s", resourceName)
		}

		// Check the actual subscription state in PostgreSQL
		config := getTestConfig(nil)
		db, err := sql.Open("postgres", config.connStr(rs.Primary.Attributes["database"]))
		if err != nil {
			return fmt.Errorf("could not connect to database: %v", err)
		}
		defer db.Close()

		var enabled bool
		err = db.QueryRow("SELECT subenabled FROM pg_subscription WHERE subname = $1", rs.Primary.Attributes["name"]).Scan(&enabled)
		if err != nil {
			return fmt.Errorf("could not query subscription state: %v", err)
		}

		if enabled == expectedEnabled {
			fmt.Printf("[DEBUG] Subscription enabled state correct: %t\n", enabled)
			return nil
		}

		fmt.Printf("[DEBUG] subscription enabled=%t, expected=%t\n", enabled, expectedEnabled)

		return fmt.Errorf("subscription enabled state did not stabilize to %t after 10 attempts", expectedEnabled)
	}
}

// testCheckReplicationOriginExists checks if a replication origin exists for the given subscription
func testCheckReplicationOriginExists(database, subscriptionName string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		config := getTestConfig(nil)

		db, err := sql.Open("postgres", config.connStr(database))
		if err != nil {
			return fmt.Errorf("could not connect to database %s: %v", database, err)
		}
		defer db.Close()

		// Get the subscription OID
		var subOid uint32
		subQuery := "SELECT oid FROM pg_subscription WHERE subname = $1"
		err = db.QueryRow(subQuery, subscriptionName).Scan(&subOid)
		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("subscription %s not found", subscriptionName)
			}
			return fmt.Errorf("could not get subscription OID: %v", err)
		}

		// Check if replication origin exists
		expectedOriginName := fmt.Sprintf("pg_%d", subOid)
		var originName string
		originQuery := "SELECT roname FROM pg_replication_origin WHERE roname = $1"
		err = db.QueryRow(originQuery, expectedOriginName).Scan(&originName)

		originExists := err == nil

		// Print debug information
		if originExists {
			fmt.Printf("[DEBUG] Replication origin %s exists for subscription %s (OID: %d)\n",
				expectedOriginName, subscriptionName, subOid)
			return nil
		} else {
			fmt.Printf("[DEBUG] No replication origin found for subscription %s (expected: %s, OID: %d)\n",
				subscriptionName, expectedOriginName, subOid)
			return fmt.Errorf("replication origin %s should exist but does not", expectedOriginName)
		}
	}
}

// cleanupReplicationOrigins removes any lingering replication origins to prevent test conflicts
func cleanupReplicationOrigins(t *testing.T) {
	config := getTestConfig(t)

	// Connect to main postgres database since replication origins are cluster-wide
	db, err := sql.Open("postgres", config.connStr("postgres"))
	if err != nil {
		t.Logf("could not connect to postgres database for cleanup: %v", err)
		return
	}
	defer db.Close()

	// Find replication origins that don't have corresponding subscriptions
	query := `
		SELECT ro.roname 
		FROM pg_replication_origin ro 
		WHERE ro.roname LIKE 'pg_%'
		AND NOT EXISTS (
			SELECT 1 
			FROM pg_subscription sub 
			WHERE ro.roname = 'pg_' || sub.oid::text
		)
	`
	rows, err := db.Query(query)
	if err != nil {
		t.Logf("could not query orphaned replication origins: %v", err)
		return
	}
	defer rows.Close()

	var orphanedOrigins []string
	for rows.Next() {
		var origin string
		if err := rows.Scan(&origin); err != nil {
			continue
		}
		orphanedOrigins = append(orphanedOrigins, origin)
	}

	for _, origin := range orphanedOrigins {
		_, err = db.Exec("SELECT pg_replication_origin_drop($1)", origin)
		if err != nil {
			t.Logf("could not drop orphaned replication origin %s: %v", origin, err)
		} else {
			t.Logf("cleaned up orphaned replication origin: %s", origin)
		}
	}
}
