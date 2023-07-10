# Windows 10 -> 11 migration broke WSL, but running update fixed it
wsl --update
wsl -d docker-desktop sh -c "sysctl -w vm.max_map_count=262144"
docker-compose up