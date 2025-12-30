const route = require('express').Router()
const pic = require('../controllers/pic')

route.post('/add', pic.addPic)
route.get('/get', pic.getPic)
route.patch('/update/:id', pic.updatePic)
route.patch('/delete', pic.deletePic)
route.get('/detail/:id', pic.getDetailPic)
route.post('/master', pic.uploadMasterPic)
route.get('/export', pic.exportSqlPic)

// auto generate pic
route.patch('/generate', pic.generateSpvPic)

module.exports = route
