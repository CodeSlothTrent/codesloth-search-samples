# Run this scipt if you receive errors while starting elasticsearch or opensearch containers
wsl -d docker-desktop sh -c "sysctl -w vm.max_map_count=262144"