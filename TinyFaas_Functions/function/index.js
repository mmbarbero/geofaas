
module.exports = (req, res) => {
 const {performance} = require('perf_hooks');
 var time = performance.now()


  res.send(time.toString());
}
