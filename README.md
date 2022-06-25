# openvpn-monitor

Tool for monitoring your openvpn server. This tool connects to monitoring port of openvpn server, 
collects and transforms data and stores it in MySQL database. Also this tool shows collected data
on the web page.

Create user and empty database on MySQL server before usage.

How to run:

```
docker run -d --name ovpn-monitor --restart=unless-stopped -e CONNECTION_STRING="mysql+mysqldb://user:password@mysql:3306/ovpnmonitor?charset=utf8mb4" -e HOST=myvpnhost -e PORT=7505 -p 8800:8888 ovpn-monitor:v1
```
