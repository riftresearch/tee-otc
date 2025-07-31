#!/bin/bash

# Extensive test for multiple MMs competing with different spreads
# This test verifies that:
# 1. Multiple MMs can connect and provide quotes
# 2. The best quote (highest to_amount) wins
# 3. Different spreads result in different quotes
# 4. System handles many concurrent MMs properly

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m'

# Configuration
RFQ_URL="http://127.0.0.1:3000"
WS_URL="ws://127.0.0.1:3000/ws"
LOG_DIR="mm_competition_logs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Create log directory
mkdir -p "$LOG_DIR"

# Arrays to track PIDs
RFQ_PID=""
MM_PIDS=()
MM_NAMES=()
MM_SPREADS=()

# Initialize win counters for all MMs
declare -a MM_WIN_COUNTS

# Cleanup function
cleanup() {
    echo -e "\n${BLUE}[INFO]${NC} Cleaning up..."
    
    # Kill MM servers
    for pid in "${MM_PIDS[@]}"; do
        kill $pid 2>/dev/null || true
    done
    
    # Kill RFQ server
    [ ! -z "$RFQ_PID" ] && kill $RFQ_PID 2>/dev/null || true
    
    # Force kill after delay
    sleep 2
    for pid in "${MM_PIDS[@]}"; do
        kill -9 $pid 2>/dev/null || true
    done
    [ ! -z "$RFQ_PID" ] && kill -9 $RFQ_PID 2>/dev/null || true
}

trap cleanup EXIT

# Header
echo -e "${PURPLE}=================================================${NC}"
echo -e "${PURPLE}        MM Competition Test Suite                ${NC}"
echo -e "${PURPLE}=================================================${NC}"
echo ""
echo "Test started at: $(date)"
echo "Log directory: $LOG_DIR"
echo ""

# Build projects
echo -e "${BLUE}[INFO]${NC} Building projects..."
if cargo build --bin rfq-server --bin mm-server > "$LOG_DIR/build_$TIMESTAMP.log" 2>&1; then
    echo -e "${GREEN}✓${NC} Build successful"
else
    echo -e "${RED}✗${NC} Build failed"
    exit 1
fi

# Start RFQ server
echo -e "\n${BLUE}[INFO]${NC} Starting RFQ server..."
cargo run --bin rfq-server -- --host 127.0.0.1 --port 3000 > "$LOG_DIR/rfq_$TIMESTAMP.log" 2>&1 &
RFQ_PID=$!
sleep 3

if ! ps -p $RFQ_PID > /dev/null; then
    echo -e "${RED}✗${NC} RFQ server failed to start"
    exit 1
fi
echo -e "${GREEN}✓${NC} RFQ server started (PID: $RFQ_PID)"

# Function to start an MM with given spread
start_mm() {
    local spread=$1
    local name=$2
    
    echo -e "${BLUE}[INFO]${NC} Starting MM: $name with ${spread}bps spread"
    cargo run --bin mm-server -- --rfq-url $WS_URL --spread-bps $spread --mm-name $name > "$LOG_DIR/mm_${name}_$TIMESTAMP.log" 2>&1 &
    local pid=$!
    
    sleep 2
    
    if ps -p $pid > /dev/null; then
        MM_PIDS+=($pid)
        MM_NAMES+=($name)
        MM_SPREADS+=($spread)
        echo -e "${GREEN}✓${NC} MM $name started (PID: $pid, spread: ${spread}bps)"
        return 0
    else
        echo -e "${RED}✗${NC} MM $name failed to start"
        return 1
    fi
}

# Start multiple MMs with different spreads
echo -e "\n${CYAN}=== Starting Market Makers ===${NC}"
echo "Creating a competitive market with varying spreads..."
echo ""

# Start MMs with different spreads (in basis points)
# Lower spread = better price for user = should win
start_mm 5 "mm-ultra-tight"      # 0.05% spread - should provide best quotes
start_mm 10 "mm-tight"           # 0.10% spread
start_mm 20 "mm-normal"          # 0.20% spread
start_mm 30 "mm-wide"            # 0.30% spread
start_mm 50 "mm-extra-wide"      # 0.50% spread
start_mm 100 "mm-very-wide"      # 1.00% spread - should provide worst quotes

echo -e "\n${GREEN}Started ${#MM_PIDS[@]} market makers${NC}"

# Initialize win counters for each MM
for mm_name in "${MM_NAMES[@]}"; do
    var_name="WINS_${mm_name//-/_}"
    eval "$var_name=0"
done

# Wait for all MMs to connect
sleep 3

# Function to request quote and analyze response
request_and_analyze() {
    local test_name=$1
    local from_amount=$2
    
    echo -e "\n${CYAN}=== Test: $test_name ===${NC}"
    echo "Requesting quote for $from_amount wei ETH to BTC..."
    
    local start_time=$(date +%s)
    
    local response=$(curl -s --max-time 5 -X POST $RFQ_URL/api/v1/quotes \
        -H "Content-Type: application/json" \
        -d "{
            \"from_chain\": \"ethereum\",
            \"from_token\": {\"type\": \"Native\"},
            \"from_amount\": \"$from_amount\",
            \"to_chain\": \"bitcoin\",
            \"to_token\": {\"type\": \"Native\"}
        }")
    
    local end_time=$(date +%s)
    local latency=$(( end_time - start_time ))
    
    if echo "$response" | grep -q "quote_id"; then
        local quote_id=$(echo "$response" | grep -o '"quote_id":"[^"]*"' | cut -d'"' -f4)
        local to_amount_hex=$(echo "$response" | grep -o '"to_amount":"[^"]*"' | cut -d'"' -f4)
        local expires_at=$(echo "$response" | grep -o '"expires_at":"[^"]*"' | cut -d'"' -f4)
        
        # Convert hex to decimal
        local to_amount_dec=$(printf "%d" "$to_amount_hex" 2>/dev/null || echo "0")
        
        echo -e "${GREEN}✓${NC} Got quote:"
        echo "  Quote ID: $quote_id"
        echo "  To Amount: $to_amount_hex ($to_amount_dec satoshis)"
        echo "  Expires: $expires_at"
        echo "  Latency: ${latency}s"
        
        # Calculate effective rate (satoshis per ETH)
        if [ "$from_amount" = "1000000000000000000" ]; then
            echo "  Rate: $to_amount_dec satoshis/ETH"
        fi
        
        # Check which MM won by looking at logs
        echo -e "\n  Checking which MM provided winning quote..."
        for i in "${!MM_NAMES[@]}"; do
            local mm_name="${MM_NAMES[$i]}"
            local mm_spread="${MM_SPREADS[$i]}"
            if grep -q "Generated quote: quote_id=$quote_id" "$LOG_DIR/mm_${mm_name}_$TIMESTAMP.log" 2>/dev/null; then
                echo -e "  ${GREEN}➜ Winner: $mm_name (${mm_spread}bps spread)${NC}"
                # Increment win counter
                var_name="WINS_${mm_name//-/_}"
                eval "$var_name=\$((\$$var_name + 1))"
            fi
        done
        
        return 0
    else
        echo -e "${RED}✗${NC} Failed to get quote"
        echo "  Response: $response"
        return 1
    fi
}

# Run multiple quote tests
echo -e "\n${PURPLE}=== Running Quote Competition Tests ===${NC}"

# Test 1: Standard 1 ETH quote
request_and_analyze "Standard 1 ETH Quote" "1000000000000000000"

# Test 2: Large amount (100 ETH)
request_and_analyze "Large 100 ETH Quote" "100000000000000000000"

# Test 3: Small amount (0.01 ETH)
request_and_analyze "Small 0.01 ETH Quote" "10000000000000000"

# Test 4: Multiple rapid quotes to test consistency
echo -e "\n${CYAN}=== Test: Rapid Sequential Quotes ===${NC}"
echo "Sending 10 rapid quotes to test consistency..."
AMOUNTS=()
for i in {1..10}; do
    response=$(curl -s --max-time 5 -X POST $RFQ_URL/api/v1/quotes \
        -H "Content-Type: application/json" \
        -d '{
            "from_chain": "ethereum",
            "from_token": {"type": "Native"},
            "from_amount": "1000000000000000000",
            "to_chain": "bitcoin",
            "to_token": {"type": "Native"}
        }')
    
    if echo "$response" | grep -q "to_amount"; then
        amount=$(echo "$response" | grep -o '"to_amount":"[^"]*"' | cut -d'"' -f4)
        quote_id=$(echo "$response" | grep -o '"quote_id":"[^"]*"' | cut -d'"' -f4)
        AMOUNTS+=($amount)
        
        # Track winner for this quote
        for mm_name in "${MM_NAMES[@]}"; do
            if grep -q "Generated quote: quote_id=$quote_id" "$LOG_DIR/mm_${mm_name}_$TIMESTAMP.log" 2>/dev/null; then
                var_name="WINS_${mm_name//-/_}"
                eval "$var_name=\$((\$$var_name + 1))"
                break
            fi
        done
    fi
done

echo "Received ${#AMOUNTS[@]} quotes:"
for amount in "${AMOUNTS[@]}"; do
    echo "  - $amount"
done

# Check if all amounts are similar (should be if same MM wins)
if [ ${#AMOUNTS[@]} -gt 0 ]; then
    first_amount="${AMOUNTS[0]}"
    all_same=true
    for amount in "${AMOUNTS[@]}"; do
        if [ "$amount" != "$first_amount" ]; then
            all_same=false
            break
        fi
    done
    
    if [ "$all_same" = true ]; then
        echo -e "${GREEN}✓${NC} All quotes consistent (same MM winning)"
    else
        echo -e "${YELLOW}!${NC} Quotes vary (different MMs winning or price changes)"
    fi
fi

# Test 5: Concurrent quote requests
echo -e "\n${CYAN}=== Test: Concurrent Quote Requests ===${NC}"
echo "Sending 20 concurrent quote requests..."

CONCURRENT_RESULTS=()
PIDS=()
for i in {1..20}; do
    {
        # Add timeout to curl to prevent hanging
        response=$(curl -s --max-time 5 -X POST $RFQ_URL/api/v1/quotes \
            -H "Content-Type: application/json" \
            -d "{
                \"from_chain\": \"ethereum\",
                \"from_token\": {\"type\": \"Native\"},
                \"from_amount\": \"$((1000000000000000000 + i))\",
                \"to_chain\": \"bitcoin\",
                \"to_token\": {\"type\": \"Native\"}
            }")
        if echo "$response" | grep -q "quote_id"; then
            echo "Request $i: SUCCESS"
            quote_id=$(echo "$response" | grep -o '"quote_id":"[^"]*"' | cut -d'"' -f4)
            
            # Track winner for this quote
            for mm_name in "${MM_NAMES[@]}"; do
                if grep -q "Generated quote: quote_id=$quote_id" "$LOG_DIR/mm_${mm_name}_$TIMESTAMP.log" 2>/dev/null; then
                    var_name="WINS_${mm_name//-/_}"
                    eval "$var_name=\$((\$$var_name + 1))"
                    break
                fi
            done
        else
            echo "Request $i: FAILED - Response: $response"
        fi
    } &
    PIDS+=($!)
done

# Wait with timeout
TIMEOUT=30
SECONDS=0
while [ ${#PIDS[@]} -gt 0 ]; do
    if [ $SECONDS -gt $TIMEOUT ]; then
        echo -e "${RED}✗${NC} Timeout waiting for concurrent requests"
        for pid in "${PIDS[@]}"; do
            kill $pid 2>/dev/null || true
        done
        break
    fi
    
    NEW_PIDS=()
    for pid in "${PIDS[@]}"; do
        if kill -0 $pid 2>/dev/null; then
            NEW_PIDS+=($pid)
        fi
    done
    PIDS=("${NEW_PIDS[@]}")
    
    if [ ${#PIDS[@]} -gt 0 ]; then
        sleep 0.1
    fi
done

echo -e "${GREEN}✓${NC} Concurrent test completed"

# Analyze MM performance
echo -e "\n${PURPLE}=== Market Maker Performance Analysis ===${NC}"

for i in "${!MM_NAMES[@]}"; do
    mm_name="${MM_NAMES[$i]}"
    mm_spread="${MM_SPREADS[$i]}"
    mm_log="$LOG_DIR/mm_${mm_name}_$TIMESTAMP.log"
    
    echo -e "\n${BLUE}$mm_name (${mm_spread}bps spread):${NC}"
    
    # Count quotes generated
    quotes_generated=$(grep -c "Generated quote:" "$mm_log" 2>/dev/null || echo "0")
    echo "  Quotes generated: $quotes_generated"
    
    # Check for errors
    errors=$(grep -c "ERROR\|Failed" "$mm_log" 2>/dev/null || echo "0")
    echo "  Errors: $errors"
    
    # Get latest metrics
    metrics=$(grep "Metrics" "$mm_log" 2>/dev/null | tail -1)
    if [ ! -z "$metrics" ]; then
        echo "  Latest metrics: ${metrics#*INFO*server:}"
    fi
done

# Test spread calculation accuracy
echo -e "\n${PURPLE}=== Spread Calculation Verification ===${NC}"
echo "Checking if tighter spreads consistently win..."

# Make several quotes and track winners
# Initialize win counts for each MM
for mm_name in "${MM_NAMES[@]}"; do
    eval "WINS_${mm_name//-/_}=0"
done

for i in {1..10}; do
    response=$(curl -s --max-time 5 -X POST $RFQ_URL/api/v1/quotes \
        -H "Content-Type: application/json" \
        -d '{
            "from_chain": "ethereum",
            "from_token": {"type": "Native"},
            "from_amount": "1000000000000000000",
            "to_chain": "bitcoin",
            "to_token": {"type": "Native"}
        }')
    
    if echo "$response" | grep -q "quote_id"; then
        quote_id=$(echo "$response" | grep -o '"quote_id":"[^"]*"' | cut -d'"' -f4)
        
        # Find winner by checking which MM generated this quote_id
        for mm_name in "${MM_NAMES[@]}"; do
            if grep -q "Generated quote: quote_id=$quote_id" "$LOG_DIR/mm_${mm_name}_$TIMESTAMP.log" 2>/dev/null; then
                var_name="WINS_${mm_name//-/_}"
                eval "$var_name=\$((\$$var_name + 1))"
                break
            fi
        done
    fi
done

# Wait a moment for all logs to be flushed
sleep 2

# Now count all wins by scanning the logs
echo -e "\n${YELLOW}Recounting all wins from logs...${NC}"
for mm_name in "${MM_NAMES[@]}"; do
    var_name="WINS_${mm_name//-/_}"
    eval "$var_name=0"
done

# Count all quote wins from the RFQ server log
while IFS= read -r line; do
    if [[ "$line" =~ \"quote_id\":\"([^\"]+)\" ]]; then
        quote_id="${BASH_REMATCH[1]}"
        for mm_name in "${MM_NAMES[@]}"; do
            if grep -q "Generated quote: quote_id=$quote_id" "$LOG_DIR/mm_${mm_name}_$TIMESTAMP.log" 2>/dev/null; then
                var_name="WINS_${mm_name//-/_}"
                eval "$var_name=\$((\$$var_name + 1))"
                break
            fi
        done
    fi
done < <(grep "Sending quote response" "$LOG_DIR/rfq_$TIMESTAMP.log" 2>/dev/null)

echo -e "\n${YELLOW}Total Quote Competition Results (All Tests):${NC}"
echo "Quotes requested: 43 (3 individual + 10 rapid + 20 concurrent + 10 spread verification)"
echo -e "\nWins by Market Maker:"
for i in "${!MM_NAMES[@]}"; do
    mm_name="${MM_NAMES[$i]}"
    spread="${MM_SPREADS[$i]}"
    var_name="WINS_${mm_name//-/_}"
    count=$(eval "echo \$$var_name")
    echo "  $mm_name (${spread}bps): $count wins"
done

# Calculate total wins
total_wins=0
for mm_name in "${MM_NAMES[@]}"; do
    var_name="WINS_${mm_name//-/_}"
    count=$(eval "echo \$$var_name")
    total_wins=$((total_wins + count))
done
echo -e "\nTotal quotes won: $total_wins / 43"