const server = require('http').createServer(function(request, response) {
	if(request.method === 'GET') {
		//For aws alb
		response.writeHead(200, {
			'Content-Type': 'text/plain',
			'Content-Length' : 2
		});
		response.write('OK');
		response.end();
	}
});
const redis = require('socket.io-redis');
const io = require('socket.io')(server);

var sent = 0;
var recieved = 0;
var connections = 0;

io.adapter(redis({host: 'ec2-13-49-49-203.eu-north-1.compute.amazonaws.com', port: 6379}));

io.on('connection', function(socket) {
	connections++;
	//Recieved a message from a publisher
	socket.on('pub_msg', function (data) {
		recieved++;
		var message = JSON.parse(data);
		var jsonObj = JSON.stringify({"m":message.m, "sid":socket.id, "pid": message.id});
		io.to(message.r).emit(jsonObj);
		sent++;
	});

	//Recieved a message from a subscriber
	socket.on('sub_msg', function (data) {
		var message = JSON.parse(data);
		//Join the requested room
		socket.join(message.sub_room);
	})

	socket.on('disconnect', function() {
		connections--;
		console.log('Socket disconnected');
	});

	//Subscriber sends a response msg to the publisher
	socket.on('response_msg', function(data) {
		recieved++;
		var message = JSON.parse(data);
		var jsonObj = JSON.stringify({"m": message.m, "pid": message.pid});
		socket.broadcast.to(message.sid).emit('response', jsonObj);
		sent++;
	});
});

//Publish broker information each 10 seconds
setInterval(function() {
	console.log("Recieved: " + recieved + " Sent: " + sent + " Connections: " + connections);
}, 10000);

//Start the server and listen to port 8080
server.listen(8080);
