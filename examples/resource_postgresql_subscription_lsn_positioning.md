# PostgreSQL Subscription with LSN Positioning

This example demonstrates how to use the enhanced PostgreSQL subscription resource with LSN (Log Sequence Number) positioning capability.

## Overview

The `start_lsn` parameter allows you to enable a disabled subscription starting from a specific point in the PostgreSQL Write-Ahead Log (WAL), enabling selective replication of data.

## Use Cases

- **Point-in-time recovery**: Start replication from a specific LSN to avoid replaying unwanted data changes
- **Incremental data sync**: Resume replication from where it left off after maintenance windows
- **Selective replication**: Begin replication only from a certain point in the timeline

## Example Configuration

### Basic Setup

```hcl
terraform {
  required_providers {
    postgresql = {
      source = "verkada/postgresql"
    }
  }
}

# Publisher database configuration
provider "postgresql" {
  alias    = "publisher"
  host     = "publisher.example.com"
  port     = 5432
  database = "publisher_db"
  username = "postgres"
  password = var.publisher_password
  sslmode  = "require"
}

# Subscriber database configuration
provider "postgresql" {
  alias    = "subscriber"
  host     = "subscriber.example.com"
  port     = 5432
  database = "subscriber_db"
  username = "postgres"
  password = var.subscriber_password
  sslmode  = "require"
}
```

### Publication on Publisher

```hcl
resource "postgresql_publication" "example" {
  provider = postgresql.publisher
  name     = "example_publication"
  tables   = ["public.users", "public.orders"]
  
  publish_insert = true
  publish_update = true
  publish_delete = true
  publish_truncate = true
}
```

### Replication Slot (if using custom slot)

```hcl
resource "postgresql_replication_slot" "example" {
  provider = postgresql.publisher
  name     = "example_replication_slot"
  plugin   = "pgoutput"
}
```

### Subscription with LSN Positioning

```hcl
# Step 1: Create subscription in disabled state
resource "postgresql_subscription" "example_disabled" {
  provider = postgresql.subscriber
  
  name         = "example_subscription"
  database     = "subscriber_db"
  conninfo     = "host=publisher.example.com port=5432 dbname=publisher_db user=replication_user password=${var.replication_password}"
  publications = [postgresql_publication.example.name]
  
  # Create disabled initially
  enabled     = false
  create_slot = false  # Use existing slot
  slot_name   = postgresql_replication_slot.example.name
  copy_data   = false  # Don't copy existing data
  connect     = true
}

# Step 2: Enable with LSN positioning
resource "postgresql_subscription" "example_enabled" {
  provider = postgresql.subscriber
  
  name         = "example_subscription"
  database     = "subscriber_db"
  conninfo     = "host=publisher.example.com port=5432 dbname=publisher_db user=replication_user password=${var.replication_password}"
  publications = [postgresql_publication.example.name]
  
  # Enable with LSN positioning
  enabled     = true
  create_slot = false
  slot_name   = postgresql_replication_slot.example.name
  copy_data   = false
  connect     = true
  start_lsn   = var.target_lsn  # e.g., "0/1234567"
  
  depends_on = [postgresql_subscription.example_disabled]
}
```

### Alternative: Single Resource with Lifecycle

```hcl
resource "postgresql_subscription" "example" {
  provider = postgresql.subscriber
  
  name         = "example_subscription"
  database     = "subscriber_db"
  conninfo     = "host=publisher.example.com port=5432 dbname=publisher_db user=replication_user password=${var.replication_password}"
  publications = [postgresql_publication.example.name]
  
  enabled     = true
  create_slot = false
  slot_name   = postgresql_replication_slot.example.name
  copy_data   = false
  connect     = true
  start_lsn   = var.target_lsn
  
  lifecycle {
    # Create disabled first, then enable with LSN
    create_before_destroy = true
  }
}
```

## Variables

```hcl
variable "publisher_password" {
  description = "Password for publisher database"
  type        = string
  sensitive   = true
}

variable "subscriber_password" {
  description = "Password for subscriber database"
  type        = string
  sensitive   = true
}

variable "replication_password" {
  description = "Password for replication user"
  type        = string
  sensitive   = true
}

variable "target_lsn" {
  description = "LSN position to start replication from"
  type        = string
  default     = null
  
  validation {
    condition = var.target_lsn == null || can(regex("^[0-9A-Fa-f]+/[0-9A-Fa-f]+$", var.target_lsn))
    error_message = "LSN must be in format 'XXX/XXXXXXX' (hexadecimal)."
  }
}
```

## How to Get LSN Values

### Get Current LSN

```sql
-- On the publisher database
SELECT pg_current_wal_lsn();
-- Example output: 0/1234567
```

### Get LSN After Specific Transaction

```sql
-- Execute your data changes
INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com');

-- Capture LSN immediately after
SELECT pg_current_wal_lsn();
```

### Monitor Replication Progress

```sql
-- Check replication slot status
SELECT slot_name, active, restart_lsn, confirmed_flush_lsn 
FROM pg_replication_slots 
WHERE slot_name = 'example_replication_slot';

-- Check subscription status
SELECT subname, subenabled, subslotname 
FROM pg_subscription 
WHERE subname = 'example_subscription';
```

## Important Notes

### LSN Positioning Rules

1. **Only works during disable → enable transition**: `start_lsn` can only be set when enabling a previously disabled subscription
2. **Cannot be used during creation**: LSN positioning is not allowed when initially creating a subscription
3. **Forward-only**: The LSN must be at or after the current replication position to prevent data inconsistency
4. **One-time operation**: LSN positioning only applies to the enable operation; subsequent enables ignore this parameter

### Best Practices

1. **Capture LSN at the right time**: Get the LSN immediately after the data changes you want to exclude from replication
2. **Test thoroughly**: Validate LSN positioning in a development environment before production use
3. **Monitor replication lag**: Check that replication catches up after enabling with LSN positioning
4. **Use transactions**: When possible, capture LSN within the same transaction as your data changes for precision

### Prerequisites

- PostgreSQL 10+ with logical replication enabled
- Replication user with appropriate permissions
- Publications configured on publisher
- Network connectivity between publisher and subscriber
- Compatible table schemas on both sides

## Error Handling

Common errors and solutions:

```hcl
# Handle connection errors
resource "postgresql_subscription" "example" {
  # ... other configuration ...
  
  # Add retry logic for connection issues
  timeouts {
    create = "5m"
    update = "5m"
    delete = "5m"
  }
}
```

## Complete Example

See the complete working example in the `subscription-lsn-positioning/` directory for a full setup including:

- Database creation
- User and permission setup  
- Publication configuration
- Subscription with LSN positioning
- Monitoring and validation queries