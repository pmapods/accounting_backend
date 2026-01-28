const joi = require('joi')
const { depo, sequelize } = require('../models')
const { pagination } = require('../helpers/pagination')
const response = require('../helpers/response')
const { Op, QueryTypes } = require('sequelize')
const readXlsxFile = require('read-excel-file/node')
const multer = require('multer')
const uploadMaster = require('../helpers/uploadMaster')
const fs = require('fs')
const excel = require('exceljs')
const vs = require('fs-extra')
const { APP_BE } = process.env

module.exports = {
  createDepo: async (req, res) => {
    try {
      const level = req.user.level
      const schema = joi.object({
        kode_depo: joi.string().required(),
        nama_depo: joi.string().required(),
        home_town: joi.string().required(),
        channel: joi.string().required(),
        distribution: joi.string().required(),
        status_depo: joi.string().required(),
        profit_center: joi.string().required(),
        kode_sap_1: joi.string().required(),
        kode_sap_2: joi.string().required(),
        kode_plant: joi.string().required(),
        nama_grom: joi.string().required(),
        nama_bm: joi.string().required(),
        nama_ass: joi.string().allow(''),
        nama_pic_1: joi.string().allow(''),
        nama_pic_2: joi.string().allow(''),
        nama_pic_3: joi.string().allow(''),
        nama_pic_4: joi.string().allow('')
      })
      const { value: results, error } = schema.validate(req.body)
      if (error) {
        return response(res, 'Error', { error: error.message }, 401, false)
      } else {
        if (level === 1) {
          const result = await depo.findAll({ where: { kode_depo: results.kode_depo } })
          if (result.length > 0) {
            return response(res, 'kode depo already use', {}, 404, false)
          } else {
            const result = await depo.findAll({ where: { kode_sap_1: results.kode_sap_1 } })
            if (result.length > 0) {
              return response(res, 'kode sap 1 already use', {}, 404, false)
            } else {
              const result = await depo.findAll({ where: { kode_sap_2: results.kode_sap_2 } })
              if (result.length > 0) {
                return response(res, 'kode sap 2 already use', {}, 404, false)
              } else {
                const result = await depo.findAll({ where: { kode_plant: results.kode_plant } })
                if (result.length > 0) {
                  return response(res, 'kode plant already use', {}, 404, false)
                } else {
                  const result = await depo.findAll({ where: { profit_center: results.profit_center } })
                  if (result.length > 0) {
                    return response(res, 'profit center already use', {}, 404, false)
                  } else {
                    const result = await depo.create(results)
                    if (result) {
                      return response(res, 'succesfully add depo', { result })
                    } else {
                      return response(res, 'failed to add depo', {}, 404, false)
                    }
                  }
                }
              }
            }
          }
        } else {
          return response(res, "you're not super administrator", {}, 404, false)
        }
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  updateDepo: async (req, res) => {
    try {
      const level = req.user.level
      const id = req.params.id
      const schema = joi.object({
        kode_depo: joi.string().required(),
        nama_depo: joi.string(),
        home_town: joi.string(),
        channel: joi.string(),
        distribution: joi.string(),
        status_depo: joi.string(),
        kode_sap_1: joi.string().required(),
        kode_sap_2: joi.string().required(),
        profit_center: joi.string().required(),
        kode_plant: joi.string().required(),
        nama_grom: joi.string().required(),
        nama_bm: joi.string().required(),
        nama_ass: joi.string().allow(''),
        nama_pic_1: joi.string().allow(''),
        nama_pic_2: joi.string().allow(''),
        nama_pic_3: joi.string().allow(''),
        nama_pic_4: joi.string().allow('')
      })
      const { value: results, error } = schema.validate(req.body)
      if (error) {
        return response(res, 'Error', { error: error.message }, 401, false)
      } else {
        if (level === 1) {
          const result = await depo.findAll({ where: { kode_depo: results.kode_depo, [Op.not]: { id: id } } })
          if (result.length > 0) {
            return response(res, 'kode depo already use', {}, 404, false)
          } else {
            const result = await depo.findAll({ where: { kode_sap_1: results.kode_sap_1, [Op.not]: { id: id } } })
            if (result.length > 0) {
              return response(res, 'kode sap 1 already use', {}, 404, false)
            } else {
              const result = await depo.findAll({ where: { kode_sap_2: results.kode_sap_2, [Op.not]: { id: id } } })
              if (result.length > 0) {
                return response(res, 'kode sap 2 already use', {}, 404, false)
              } else {
                const result = await depo.findAll({ where: { kode_plant: results.kode_plant, [Op.not]: { id: id } } })
                if (result.length > 0) {
                  return response(res, 'kode plant already use', {}, 404, false)
                } else {
                  const result = await depo.findAll({ where: { profit_center: results.profit_center, [Op.not]: { id: id } } })
                  if (result.length > 0) {
                    return response(res, 'profit center already use', {}, 404, false)
                  } else {
                    const result = await depo.findByPk(id)
                    if (result) {
                      await result.update(results)
                      return response(res, 'succesfully update depo', { result })
                    } else {
                      return response(res, 'failed to update depo', {}, 404, false)
                    }
                  }
                }
              }
            }
          }
        } else {
          return response(res, "you're not super administrator", {}, 404, false)
        }
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  deleteDepo: async (req, res) => {
    try {
      const level = req.user.level
      const { listId } = req.body
      console.log(req.body)
      if (level === 1) {
        if (listId !== undefined && listId.length > 0) {
          const cekData = []
          for (let i = 0; i < listId.length; i++) {
            const result = await depo.findByPk(listId[i])
            if (result) {
              await result.destroy()
              cekData.push(result)
            }
          }
          if (cekData.length > 0) {
            return response(res, 'success delete depo', { result: cekData })
          } else {
            return response(res, 'depo not found', {}, 404, false)
          }
        } else {
          return response(res, 'depo not found', {}, 404, false)
        }
      } else {
        return response(res, "You're not super administrator", {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  getDepo: async (req, res) => {
    try {
      let { limit, page, search, sort } = req.query
      let searchValue = ''
      let sortValue = ''
      if (typeof search === 'object') {
        searchValue = Object.values(search)[0]
      } else {
        searchValue = search || ''
      }
      if (typeof sort === 'object') {
        sortValue = Object.values(sort)[0]
      } else {
        sortValue = sort || 'id'
      }
      if (!limit) {
        limit = 10
      } else {
        limit = parseInt(limit)
      }
      if (!page) {
        page = 1
      } else {
        page = parseInt(page)
      }
      const result = await depo.findAndCountAll({
        where: {
          [Op.or]: [
            { kode_depo: { [Op.like]: `%${searchValue}%` } },
            { nama_depo: { [Op.like]: `%${searchValue}%` } },
            { home_town: { [Op.like]: `%${searchValue}%` } },
            { channel: { [Op.like]: `%${searchValue}%` } },
            { distribution: { [Op.like]: `%${searchValue}%` } },
            { status_depo: { [Op.like]: `%${searchValue}%` } },
            { profit_center: { [Op.like]: `%${searchValue}%` } },
            { kode_sap_1: { [Op.like]: `%${searchValue}%` } },
            { kode_sap_2: { [Op.like]: `%${searchValue}%` } },
            { kode_plant: { [Op.like]: `%${searchValue}%` } },
            { nama_grom: { [Op.like]: `%${searchValue}%` } },
            { nama_bm: { [Op.like]: `%${searchValue}%` } },
            { nama_ass: { [Op.like]: `%${searchValue}%` } },
            { nama_pic_1: { [Op.like]: `%${searchValue}%` } },
            { nama_pic_2: { [Op.like]: `%${searchValue}%` } },
            { nama_pic_3: { [Op.like]: `%${searchValue}%` } },
            { nama_pic_4: { [Op.like]: `%${searchValue}%` } }
          ]
        },
        order: [[sortValue, 'ASC']],
        limit: limit,
        offset: (page - 1) * limit
      })
      const pageInfo = pagination('/depo/get', req.query, page, limit, result.count)
      if (result) {
        return response(res, 'list users', { result, pageInfo })
      } else {
        return response(res, 'failed to get user', {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  getDetailDepo: async (req, res) => {
    try {
      const id = req.params.id
      const schema = joi.object({
        kode_depo: joi.number()
      })
      const { value: results, error } = schema.validate(req.body)
      if (error) {
        return response(res, 'Error', { error: error.message }, 401, false)
      } else {
        if (results.kode_depo) {
          const result = await depo.findOne({ where: { kode_depo: results.kode_depo } })
          if (result) {
            return response(res, 'succes get detail depo', { result })
          } else {
            return response(res, 'failed get detail depo', {}, 404, false)
          }
        } else {
          const result = await depo.findByPk(id)
          if (result) {
            return response(res, 'succes get detail depo', { result })
          } else {
            return response(res, 'failed get detail depo', {}, 404, false)
          }
        }
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  uploadMasterDepo: async (req, res) => {
    const level = req.user.level
    if (level === 1) {
      uploadMaster(req, res, async function (err) {
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
          const dokumen = `assets/masters/${req.files[0].filename}`
          const rows = await readXlsxFile(dokumen)
          const count = []
          const cek = [
            'Kode Depo',
            'Nama Depo',
            'Home Town',
            'Channel',
            'Distribution',
            'Status Depo',
            'Profit Center',
            'Kode SAP 1',
            'Kode SAP 2',
            'Kode Plant',
            'Nama GROM',
            'Nama BM',
            'Nama ASS',
            'Nama PIC 1',
            'Nama PIC 2',
            'Nama PIC 3',
            'Nama PIC 4'
          ]
          const valid = rows[0]
          for (let i = 0; i < cek.length; i++) {
            console.log(valid[i] === cek[i])
            if (valid[i] === cek[i]) {
              count.push(1)
            }
          }
          console.log(count.length)
          if (count.length === cek.length) {
            const plant = []
            const profit = []
            const sap1 = []
            const sap2 = []
            const kodeDepo = []
            const kode = []
            for (let i = 1; i < rows.length; i++) {
              const a = rows[i]
              plant.push(`Kode Plant ${a[9]}`)
              kode.push(`${a[9]}`)
              kodeDepo.push(`Kode Depo ${a[0]}`)
              // profit.push(`Profit Center ${a[6]}`)
              // if (a[7] !== null) {
              //   sap1.push(`Kode SAP 1 ${a[7]}`)
              // }
              // if (a[8] !== null) {
              //   sap2.push(`Kode SAP 2 ${a[8]}`)
              // }
            }
            const object = {}
            const result = []
            const dupProfit = {}
            const dupSap1 = {}
            const dupSap2 = {}
            const dupDepo = {}

            kodeDepo.forEach(item => {
              if (!dupDepo[item]) { dupDepo[item] = 0 }
              dupDepo[item] += 1
            })

            for (const prop in dupDepo) {
              if (dupDepo[prop] >= 2) {
                result.push(prop)
              }
            }

            profit.forEach(item => {
              if (!dupProfit[item]) { dupProfit[item] = 0 }
              dupProfit[item] += 1
            })

            for (const prop in dupProfit) {
              if (dupProfit[prop] >= 2) {
                result.push(prop)
              }
            }

            sap1.forEach(item => {
              if (!dupSap1[item]) { dupSap1[item] = 0 }
              dupSap1[item] += 1
            })

            for (const prop in dupSap1) {
              if (dupSap1[prop] >= 2) {
                result.push(prop)
              }
            }

            sap2.forEach(item => {
              if (!dupSap2[item]) { dupSap2[item] = 0 }
              dupSap2[item] += 1
            })

            for (const prop in dupSap2) {
              if (dupSap2[prop] >= 2) {
                result.push(prop)
              }
            }

            plant.forEach(item => {
              if (!object[item]) { object[item] = 0 }
              object[item] += 1
            })

            for (const prop in object) {
              if (object[prop] >= 2) {
                result.push(prop)
              }
            }
            if (result.length > 0) {
              return response(res, 'there is duplication in your file master', { result }, 404, false)
            } else {
              rows.shift()
              const arr = []
              for (let i = 0; i < rows.length; i++) {
                const dataDepo = rows[i]
                const data = {
                  kode_depo: dataDepo[0],
                  nama_depo: dataDepo[1],
                  home_town: dataDepo[2],
                  channel: dataDepo[3],
                  distribution: dataDepo[4],
                  status_depo: dataDepo[5],
                  profit_center: dataDepo[6],
                  kode_sap_1: dataDepo[7],
                  kode_sap_2: dataDepo[8],
                  kode_plant: dataDepo[9],
                  nama_grom: dataDepo[10],
                  nama_bm: dataDepo[11],
                  nama_ass: dataDepo[12],
                  nama_pic_1: dataDepo[13],
                  nama_pic_2: dataDepo[14],
                  nama_pic_3: dataDepo[15],
                  nama_pic_4: dataDepo[16]
                }
                const select = await depo.findOne({
                  where: {
                    kode_depo: data.kode_depo
                  }
                })
                if (select) {
                  await select.update(data)
                  arr.push(select)
                } else {
                  await depo.create(data)
                  arr.push(data)
                }
              }
              if (arr.length > 0) {
                fs.unlink(dokumen, function (err) {
                  if (err) throw err
                  console.log('success')
                })
                return response(res, 'successfully upload file master')
              } else {
                fs.unlink(dokumen, function (err) {
                  if (err) throw err
                  return response(res, 'successfully upload file master')
                })
              }
            }
          } else {
            fs.unlink(dokumen, function (err) {
              if (err) throw err
              console.log('success')
            })
            return response(res, 'Gagal upload file master, mohon gunakan template upload yang telah disediakan', {}, 400, false)
          }
        } catch (error) {
          return response(res, error.message, {}, 500, false)
        }
      })
    } else {
      return response(res, "You're not super administrator", {}, 404, false)
    }
  },
  exportSqlDepo: async (req, res) => {
    try {
      const result = await depo.findAll()
      if (result) {
        const workbook = new excel.Workbook()
        const worksheet = workbook.addWorksheet()
        const arr = []
        const header = [
          'Kode Depo',
          'Nama Depo',
          'Home Town',
          'Channel',
          'Distribution',
          'Status Depo',
          'Profit Center',
          'Kode SAP 1',
          'Kode SAP 2',
          'Kode Plant',
          'Nama GROM',
          'Nama BM',
          'Nama ASS',
          'Nama PIC 1',
          'Nama PIC 2',
          'Nama PIC 3',
          'Nama PIC 4'
        ]
        const key = [
          'kode_depo',
          'nama_depo',
          'home_town',
          'channel',
          'distribution',
          'status_depo',
          'profit_center',
          'kode_sap_1',
          'kode_sap_2',
          'kode_plant',
          'nama_grom',
          'nama_bm',
          'nama_ass',
          'nama_pic_1',
          'nama_pic_2',
          'nama_pic_3',
          'nama_pic_4'
        ]
        for (let i = 0; i < header.length; i++) {
          let temp = { header: header[i], key: key[i] }
          arr.push(temp)
          temp = {}
        }
        worksheet.columns = arr
        const cek = worksheet.addRows(result)
        if (cek) {
          const name = new Date().getTime().toString().concat('-depo').concat('.xlsx')
          await workbook.xlsx.writeFile(name)
          vs.move(name, `assets/exports/${name}`, function (err) {
            if (err) {
              throw err
            }
            console.log('success')
          })
          return response(res, 'success', { link: `${APP_BE}/download/${name}` })
        } else {
          return response(res, 'failed create file', {}, 404, false)
        }
      } else {
        return response(res, 'failed', {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  }
}
