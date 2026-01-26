const route = require('express').Router()
const salesConsole = require('../controllers/salesConsole')

route.post('/upload', salesConsole.uploadSalesConsole)
route.post('/update', salesConsole.updateSalesConsole)
route.patch('/delete', salesConsole.deleteReport)
route.get('/get', salesConsole.getAllReport)
route.get('/detail/:id', salesConsole.getDetailReport)

module.exports = route
