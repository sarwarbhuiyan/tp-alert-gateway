#!/bin/bash
# Setup Timeplus user from within the Docker container
# This script connects to the Timeplus Docker container and creates a test user

echo "Connecting to Timeplus Docker container to create test user..."

# Connect to the container and create the user
docker exec -it timeplus bash -c "
echo 'Connecting to Timeplus with proton user...'
echo 'Creating test user...'
/timeplus/bin/timeplusd client --user proton --password 'timeplus@t+' --query=\"CREATE USER IF NOT EXISTS test IDENTIFIED BY 'test123'\"
echo 'Granting permissions...'
/timeplus/bin/timeplusd client --user proton --password 'timeplus@t+' --query=\"GRANT ALL ON *.* TO test\"
echo 'Verifying user was created...'
/timeplus/bin/timeplusd client --user proton --password 'timeplus@t+' --query=\"SELECT name FROM system.users WHERE name = 'test'\"
"

echo "User setup complete!" 