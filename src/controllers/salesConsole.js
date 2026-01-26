/* eslint-disable node/handle-callback-err */
const { sales_console } = require('../models')
const { Op } = require('sequelize')
const response = require('../helpers/response')
const { pagination } = require('../helpers/pagination')
const uploadSales = require('../helpers/uploadSales')
const multer = require('multer')
const moment = require('moment')
const fs = require('fs')

module.exports = {
  getAllReport: async (req, res) => {
    try {
      let { limit, page, search, sort, date, type } = req.query
      // let searchValue = ''
      let sortValue = ''
      if (typeof search === 'object') {
        // searchValue = Object.values(search)[0]
      } else {
        // searchValue = search || ''
      }
      if (typeof sort === 'object') {
        sortValue = Object.values(sort)[0]
      } else {
        sortValue = sort || 'id'
      }
      if (!limit) {
        limit = 10
      } else if (limit === 'all') {
        const findLimit = await sales_console.findAll()
        limit = findLimit.length
      } else {
        limit = parseInt(limit)
      }
      if (!page) {
        page = 1
      } else {
        page = parseInt(page)
      }
      const startDate = moment(date).startOf('month').toDate()
      const endDate = moment(date).endOf('month').toDate()
      const findInventory = await sales_console.findAndCountAll({
        where: {
          [Op.and]: [
            {
              [Op.or]: [
                { name: { [Op.like]: `%${search}%` } },
                { type: { [Op.like]: `%${search}%` } }
              ]
            },
            {
              date_report: { [Op.between]: [startDate, endDate] }
            },
            {
              status: type
            }
          ]
        },
        order: [[sortValue, 'DESC']]
        // limit: limit,
        // offset: (page - 1) * limit
      })
      const pageInfo = pagination('/report-inv/get', req.query, page, limit, findInventory.count)
      if (findInventory) {
        return response(res, 'succes get report inv', { result: findInventory, pageInfo, startDate, endDate })
      } else {
        return response(res, 'failed get report inv', { result: [], pageInfo })
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  getDetailReport: async (req, res) => {
    try {
      const id = req.params.id
      const findInventory = await sales_console.findByPk(id)
      if (findInventory) {
        return response(res, 'succes get detail report inv', { result: findInventory })
      } else {
        return response(res, 'failed get report inv', {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  deleteReport: async (req, res) => {
    try {
      // const level = req.user.level
      const { listId } = req.body
      console.log(req.body)
      // if (level === 1) {
      if (listId !== undefined && listId.length > 0) {
        const cekData = []
        for (let i = 0; i < listId.length; i++) {
          const result = await sales_console.findByPk(listId[i])
          if (result) {
            const path = result.path
            fs.unlink(path, function (err) {
              console.log('success')
            })
            await result.destroy()
            cekData.push(result)
          }
        }
        if (cekData.length > 0) {
          return response(res, 'success delete report inv', { result: cekData })
        } else {
          return response(res, 'report inv not found', {}, 404, false)
        }
      } else {
        return response(res, 'report inv not found', {}, 404, false)
      }
      // } else {
      //   return response(res, "You're not super administrator", {}, 404, false)
      // }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  uploadSalesConsole: async (req, res) => {
    const level = req.user.level // eslint-disable-line
    const username = req.user.name
    // if (level === 1) {
    uploadSales(req, res, async function (err) {
      try {
        if (err instanceof multer.MulterError) {
          if (err.code === 'LIMIT_UNEXPECTED_FILE' && req.files.length === 0) {
            console.log(err.code === 'LIMIT_UNEXPECTED_FILE' && req.files.length > 0)
            return response(res, 'fieldname doesnt match', {}, 500, false)
          }
          return response(res, err.message, {}, 500, false)
        } else if (err) {
          return response(res, err.message, {}, 401, false)
        }
        const { name, type, note, date_report } = req.body
        const dokumen = `assets/masters/${req.files[0].filename}`
        const data = {
          name: name,
          type: type,
          path: dokumen,
          status: 1,
          note: note,
          date_report: date_report,
          user_upload: username
        }
        const createData = await sales_console.create(data)
        if (createData) {
          return response(res, 'success upload report')
        }
      } catch (error) {
        return response(res, error.message, {}, 500, false)
      }
    })
    // } else {
    //   return response(res, "You're not super administrator", {}, 404, false)
    // }
  },
  updateSalesConsole: async (req, res) => {
    const level = req.user.level // eslint-disable-line
    const username = req.user.name
    // if (level === 1) {
    const { type } = req.query
    if (type === 'upload') {
      uploadSales(req, res, async function (err) {
        try {
          if (err instanceof multer.MulterError) {
            if (err.code === 'LIMIT_UNEXPECTED_FILE' && req.files.length === 0) {
              console.log(err.code === 'LIMIT_UNEXPECTED_FILE' && req.files.length > 0)
              return response(res, 'fieldname doesnt match', {}, 500, false)
            }
            return response(res, err.message, {}, 500, false)
          } else if (err) {
            return response(res, err.message, {}, 401, false)
          }
          const { name, type, date_report, note, id } = req.body
          const dokumen = `assets/masters/${req.files[0].filename}`
          const data = {
            name: name,
            type: type,
            path: dokumen,
            note: note,
            date_report: date_report,
            user_upload: username
          }
          const findId = await sales_console.findByPk(id)
          if (findId) {
            const updateData = await findId.update(data)
            if (updateData) {
              return response(res, 'success upload report')
            }
          }
        } catch (error) {
          return response(res, error.message, {}, 500, false)
        }
      })
    } else {
      const { name, type, date_report, note, id } = req.body
      console.log(name)
      const data = {
        name: name,
        type: type,
        note: note,
        date_report: date_report,
        user_upload: username
      }
      const findId = await sales_console.findByPk(id)
      if (findId) {
        const updateData = await findId.update(data)
        if (updateData) {
          return response(res, 'success upload report')
        }
      }
    }
    // } else {
    //   return response(res, "You're not super administrator", {}, 404, false)
    // }
  }
}
