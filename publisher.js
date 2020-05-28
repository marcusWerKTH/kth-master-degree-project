var myArgs = process.argv.slice(2);
/*myArgs[0] = number of instances to create,
  myArgs[1] = how often the instance should publish,
  myArgs[2] = the number of subscriber are connected to the brokers
*/
function pubInstance(serverHttp, interval, nrSubscribers, id) {
	var startTime = 0;
	var firstTime = true;

	var socket = require('socket.io-client')(serverHttp, {
		forceNew: true,
		//Skip the HTTP handshake and upgrade part
		transports: ['websocket']
	});

	socket.on('connect', function() {
		if(firstTime) {
			startTime = Date.now();
			firstTime = false;
		}
		//Run for 15 minutes
		var stopInterval = setInterval(function() {
			if((Date.now() - startTime)/1000 > 900) {
				console.log("Stop! ClearInterval");
				clearInterval(stopInterval);
			}
			console.log((Date.now() - startTime)/1000 + " Publisher " + id + " sent a message");
			//Randomise which topic to send to
			var randIndx = Math.floor(Math.random() * nrSubscribers) + 1  
			var time = Date.now();
			var room = "Sub" + randIndx
			var jsonObj = JSON.stringify({"r":randIndx, "m":time, "id": id});
			socket.emit('pub_msg', jsonObj);
		}, interval);

		socket.on('response', function(data) {
			var message = JSON.parse(data);
			var timeDiff = Date.now() - message.m;
			console.log((Date.now() - startTime)/1000 + " Publisher " + id + " recieved a message");
			console.log("Time diff, id " + id + ": " + (timeDiff)/1000 + " seconds, and msg id: " + message.pid);
		});
	});	
}

//If we have provided all the necessary arguments
if(myArgs.length == 3) {
	for(i = 0; i < myArgs[0]; i++) {
		pubInstance("http://DNS_adress_of_load_balancer:8080", myArgs[1], myArgs[2], i);
	}
}

