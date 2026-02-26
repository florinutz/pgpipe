#!/usr/bin/env bash
set -euo pipefail

DB="postgres://pgcdc:pgcdc@postgres:5432/pgcdc?sslmode=disable"

# Wait for postgres to be ready
echo "Waiting for PostgreSQL..."
until pg_isready -h postgres -U pgcdc -d pgcdc -q 2>/dev/null; do
  sleep 1
done
echo "PostgreSQL is ready."

# Give pgcdc a moment to set up the publication and replication slot
sleep 3

PRODUCTS=("Widget" "Gadget" "Doohickey" "Thingamajig" "Gizmo" "Sprocket" "Cog" "Bolt" "Nut" "Washer")
NAMES=("Alice" "Bob" "Carol" "Dave" "Eve" "Frank" "Grace" "Hank" "Ivy" "Jack")
TIERS=("standard" "premium" "enterprise")
STATUSES=("confirmed" "shipped" "delivered")

order_id=0
customer_id=0
cycle=0

while true; do
  cycle=$((cycle + 1))

  # Insert a new order (every cycle)
  name="${NAMES[$((RANDOM % ${#NAMES[@]}))]}"
  product="${PRODUCTS[$((RANDOM % ${#PRODUCTS[@]}))]}"
  qty=$(( (RANDOM % 5) + 1 ))
  price=$(printf "%d.%02d" $(( (RANDOM % 200) + 5 )) $(( RANDOM % 100 )))

  psql "$DB" -q -c \
    "INSERT INTO orders (customer_name, product, quantity, price) VALUES ('$name', '$product', $qty, $price);"
  order_id=$((order_id + 1))
  echo "[datagen] Inserted order #$order_id: $name bought $qty x $product @ \$$price"

  # Every 5th cycle, update a random order status
  if (( cycle % 5 == 0 && order_id > 1 )); then
    target=$(( (RANDOM % order_id) + 1 ))
    new_status="${STATUSES[$((RANDOM % ${#STATUSES[@]}))]}"
    psql "$DB" -q -c \
      "UPDATE orders SET status = '$new_status' WHERE id = $target;"
    echo "[datagen] Updated order #$target -> status=$new_status"
  fi

  # Every 3rd cycle, insert a customer
  if (( cycle % 3 == 0 )); then
    cname="${NAMES[$((RANDOM % ${#NAMES[@]}))]}"
    tier="${TIERS[$((RANDOM % ${#TIERS[@]}))]}"
    email=$(echo "$cname" | tr '[:upper:]' '[:lower:]')_${RANDOM}@example.com
    psql "$DB" -q -c \
      "INSERT INTO customers (name, email, tier) VALUES ('$cname', '$email', '$tier');"
    customer_id=$((customer_id + 1))
    echo "[datagen] Inserted customer #$customer_id: $cname ($tier)"
  fi

  sleep 2
done
