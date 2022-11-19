docker run -d --name ovpn-monitor --restart=unless-stopped -e CONNECTION_STRING="mysql+mysqldb://user:password@mysql:3306/ovpnmonitor?charset=utf8mb4" -p 8800:8888 ovpn-monitor:v2
