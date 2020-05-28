var myArgs = process.argv.slice(2);
/*myArgs[0] = number of subscribers to create,
*/

function subInstance(serverHttp, id) {
	
	var socket = require('socket.io-client')(serverHttp, {
		forceNew: true,
		//Skip the HTTP handshake and upgrade part
		transports: ['websocket']
	});

	socket.on('connect', function() {
		console.log('connected to the server!');
		var room = "Sub" + id;
		var jsonObj = JSON.stringify({"sub_room":room});
		socket.emit('sub_msg', jsonObj);

		socket.on('publish', function(data) {
			socket.emit('response_msg', data);
		});
	});	
}

//If we have provided all the necessary arguments
if(myArgs.length == 1) {
	for(i = 0; i < myArgs[0]; i++) {
		subInstance("http://DNS_adress_of_load_balancer:8080", i);
	}
}
