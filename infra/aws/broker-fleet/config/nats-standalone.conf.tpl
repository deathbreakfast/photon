listen: 0.0.0.0:4222
http: 0.0.0.0:8222
server_name: @SERVER_NAME@

jetstream {
  store_dir: @STORE_DIR@
  max_mem: 512M
  max_file: 10G
}
