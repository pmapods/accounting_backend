const route = require('express').Router()
const endstock = require('../controllers/endstock')

route.post('/report/upload', endstock.uploadReportEnd)
route.post('/report/update', endstock.updateReportEnd)
route.patch('/report/delete', endstock.deleteReport)
route.get('/report/get', endstock.getAllReport)
route.get('/report/detail/:id', endstock.getDetailReport)
route.patch('/report/generate', endstock.generateInventoryReport)
route.patch('/report/merge', endstock.mergeInventoryReports)

module.exports = route
