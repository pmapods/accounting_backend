const { pagination } = require('../helpers/pagination')
const { documents, Path, depo, activity, pic, email, notif, date_clossing, sequelize } = require('../models') // eslint-disable-line
const { Op, QueryTypes } = require('sequelize')
const response = require('../helpers/response')
const joi = require('joi')
const uploadHelper = require('../helpers/upload')
const uploadHelperMultiple = require('../helpers/uploadMultiple')
const multer = require('multer')
const fs = require('fs')
// const vs = require('fs-extra')
// const { APP_URL } = process.env
// const mailer = require('../helpers/mailer')
const moment = require('moment')
// const xlsx = require('xlsx')
const wrapMail = require('../helpers/wrapMail')
const archiver = require('archiver')
const path = require('path')
// const { fn, col } = require('sequelize')

// const buildFilter = async (level, results, depoKode, names) => {
//   if (level === 1 || level === 2 || level === 3) {
//     if (results.pic && results.pic !== 'all') {
//       const picRecords = await pic.findAll({
//         where: { pic: { [Op.like]: `%${results.pic}%` } }
//       })
//       const kodeDepos = picRecords.map(p => p.kode_depo)
//       return { kode_plant: { [Op.in]: kodeDepos } }
//     }
//     if (results.kode_plant && results.kode_plant !== 'all') {
//       return { kode_plant: results.kode_plant }
//     }
//     if (results.spv) {
//       const picRecords = await pic.findAll({
//         where: { spv: { [Op.like]: `%${results.spv}%` } }
//       })
//       const kodeDepos = picRecords.map(p => p.kode_depo)
//       return { kode_plant: { [Op.in]: kodeDepos } }
//     }
//     return {}
//   }

//   if (level === 4 || level === 5) {
//     return { kode_plant: depoKode }
//   }

//   return {}
// }

const buildHeader = (dokumenNames) => {
  const first = [
    'No',
    'Document Date', // ⬅️ tambahan
    'Nama Depo',
    'Kode Plant',
    'Profit Center',
    'Kode SAP 1',
    'Status Depo'
  ]
  const last = ['Jumlah Dokumen', 'Progress', 'Persentase']
  return first.concat(dokumenNames, last)
}

// const buildBody = (sa, dokumenNames) => {
//   const rows = []

//   sa.forEach((item, index) => {
//     const totalDoc = dokumenNames.length // total dokumen per depo

//     if (item.active?.length) {
//       item.active.forEach((act, idx) => {
//         const row = []
//         row.push(`${index + 1}.${idx + 1}`)
//         row.push(moment(act.createdAt).format('LL'))
//         row.push(item.nama_depo)
//         row.push(item.kode_plant)
//         row.push(item.profit_center)
//         row.push(item.kode_sap_1)
//         row.push(item.status_depo)

//         let progress = 0

//         for (const nama of dokumenNames) {
//           const docMatch = act.doc?.find(d => d.dokumen === nama)
//           if (docMatch) {
//             if (docMatch.status_dokumen === 3 || docMatch.status_dokumen === 5) {
//               progress++ // dihitung tiap activity
//             }
//             row.push(
//               docMatch.status_dokumen === 3
//                 ? moment(docMatch.createdAt).format('LLL')
//                 : docMatch.status_dokumen === 5
//                   ? `Telat (${moment(docMatch.createdAt).format('LLL')})`
//                   : '-'
//             )
//           } else {
//             row.push('-')
//           }
//         }

//         const percent = totalDoc > 0 ? `${Math.round((progress / totalDoc) * 100)}%` : '0%'
//         row.push(totalDoc, progress, percent)

//         rows.push(row)
//       })
//     } else {
//       // activity kosong
//       const row = []
//       row.push(index + 1)
//       row.push('-')
//       row.push(item.nama_depo)
//       row.push(item.kode_plant)
//       row.push(item.profit_center)
//       row.push(item.kode_sap_1)
//       row.push(item.status_depo)

//       for (const nama of dokumenNames) { // eslint-disable-line
//         row.push('-')
//       }

//       row.push(totalDoc, 0, '0%')
//       rows.push(row)
//     }
//   })

//   return rows
// }

module.exports = {
  getDashboardOld: async (req, res) => {
    try {
      let { limit, page, search, sort, typeSort, time, tipe, find } = req.query
      let searchValue = ''
      let sortValue = ''
      let typeSortValue = ''
      let timeValue = ''
      let tipeValue = ''
      let findValue = ''
      if (typeof search === 'object') {
        searchValue = Object.values(search)[0]
      } else {
        searchValue = search || ''
      }
      if (typeof find === 'object') {
        findValue = Object.values(find)[0]
      } else {
        findValue = find || ''
      }
      if (typeof sort === 'object') {
        sortValue = Object.values(sort)[0]
      } else {
        sortValue = sort || 'id'
      }
      if (typeof time === 'object') {
        timeValue = Object.values(time)[0]
      } else {
        timeValue = time || ''
      }
      if (typeof tipe === 'object') {
        tipeValue = Object.values(tipe)[0]
      } else {
        tipeValue = tipe || 'daily'
      }
      if (typeof typeSort === 'object') {
        typeSortValue = Object.values(typeSort)[0]
      } else {
        typeSortValue = typeSort || 'DESC'
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
      //   const id = req.user.id
      const level = req.user.level
      const kode = req.user.kode
      let timeUser = new Date(moment().format('YYYY-MM-DD 00:00'))
      let timeUserTomo = new Date(moment().add(1, 'days').format('YYYY-MM-DD 00:00'))
      let now = timeValue ? moment(timeValue).startOf('day').toDate() : moment().startOf('day').toDate()
      let tomo = timeValue ? moment(timeValue).add(1, 'days').startOf('day').toDate() : moment().add(1, 'days').startOf('day').toDate()
      if (level === 4) {
        const result = await depo.findOne({
          where: {
            [Op.or]: [
              { kode_plant: kode },
              { kode_depo: kode }
            ]
          }
        })
        if (result) {
          const cabang = result.status_depo
          const results = await documents.findAndCountAll({
            where: {
              [Op.or]: [
                { nama_dokumen: { [Op.like]: `%${searchValue}%` } }
              ],
              [Op.and]: [
                {
                  [Op.or]: [
                    { access: { [Op.like]: `%${kode}%` } },
                    { access: null }
                  ]
                },
                { status_depo: cabang },
                { uploadedBy: 'sa' },
                { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } }
              ],
              [Op.not]: { status: 'inactive' }
            },
            order: [[sortValue, typeSortValue]],
            limit: 100,
            offset: (page - 1) * 100
          })
          const pageInfo = pagination('/dokumen/get', req.query, page, limit, results.count)
          if (results) {
            if (tipeValue === 'monthly') {
              timeUser = new Date(moment().startOf('month').format('YYYY-MM-DD'))
              timeUserTomo = new Date(moment().add(1, 'month').startOf('month').format('YYYY-MM-DD'))
            }
            const cek = await activity.findAll({
              where: {
                [Op.and]: [
                  { kode_plant: kode },
                  { tipe: 'sa' },
                  { jenis_dokumen: tipeValue }
                ],
                createdAt: {
                  [Op.gt]: timeUser,
                  [Op.lt]: timeUserTomo
                }
              }
            })
            if (cek.length > 0) {
              return response(res, 'list dokumen', { results, pageInfo, cek })
            } else {
              if (tipeValue === 'daily') {
                const now = new Date(moment().startOf('month').format('YYYY-MM-DD'))
                const tomo = new Date(moment().add(1, 'month').startOf('month').format('YYYY-MM-DD'))
                const find = await activity.findAll({
                  where: {
                    [Op.and]: [
                      { kode_plant: kode },
                      { tipe: 'sa' },
                      { jenis_dokumen: tipeValue }
                    ],
                    createdAt: {
                      [Op.lt]: tomo,
                      [Op.gt]: now
                    }
                  }
                })
                if (find) {
                  const temp = []
                  find.map(item => {
                    return temp.push(item.id)
                  })
                  for (let i = 0; i < find.length; i++) {
                    const send = {
                      access: 'lock'
                    }
                    const change = await activity.findByPk(temp[i])
                    if (change) {
                      await change.update(send)
                    }
                  }
                  const data = {
                    kode_plant: kode,
                    status: 'Belum Upload',
                    documentDate: new Date(moment().subtract(1, 'days')),
                    access: 'unlock',
                    jenis_dokumen: tipeValue === 'daily' ? 'daily' : 'monthly',
                    tipe: 'sa'
                  }
                  const create = await activity.create(data)
                  if (create) {
                    return response(res, 'list dokumen', { results, pageInfo, cek: cek })
                  } else {
                    return response(res, 'failed to get dokumen', {}, 404, false)
                  }
                } else {
                  const data = {
                    kode_plant: kode,
                    status: 'Belum Upload',
                    documentDate: new Date(moment().subtract(1, 'days')),
                    access: 'unlock',
                    jenis_dokumen: tipeValue === 'daily' ? 'daily' : 'monthly',
                    tipe: 'sa'
                  }
                  const create = await activity.create(data)
                  if (create) {
                    return response(res, 'list dokumen', { results, pageInfo, cek: cek })
                  } else {
                    return response(res, 'failed to get dokumen', {}, 404, false)
                  }
                }
              } else {
                const data = {
                  kode_plant: kode,
                  status: 'Belum Upload',
                  documentDate: new Date(moment().subtract(1, 'days')),
                  access: 'unlock',
                  jenis_dokumen: tipeValue === 'daily' ? 'daily' : 'monthly',
                  tipe: 'sa'
                }
                const create = await activity.create(data)
                if (create) {
                  return response(res, 'list dokumen', { results, pageInfo, cek: cek })
                } else {
                  return response(res, 'failed to get dokumen', {}, 404, false)
                }
              }
            }
          } else {
            return response(res, 'failed to get user', {}, 404, false)
          }
        } else {
          return response(res, 'user tidak terhubung dengan depo manapun', {}, 404, false)
        }
      } else if (level === 5) {
        const result = await depo.findOne({
          where: {
            [Op.or]: [
              { kode_plant: kode },
              { kode_depo: kode }
            ]
          }
        })
        if (result) {
          const cabang = result.status_depo
          const results = await documents.findAndCountAll({
            where: {
              [Op.or]: [
                { nama_dokumen: { [Op.like]: `%${searchValue}%` } }
              ],
              [Op.and]: [
                {
                  [Op.or]: [
                    { access: { [Op.like]: `%${kode}%` } },
                    { access: null }
                  ]
                },
                { status_depo: cabang },
                { uploadedBy: 'kasir' },
                { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } }
              ],
              [Op.not]: { status: 'inactive' }
            },
            order: [[sortValue, typeSortValue]]
          })
          const pageInfo = pagination('/dokumen/get', req.query, page, limit, results.count)
          if (results) {
            if (tipeValue === 'monthly') {
              timeUser = new Date(moment().startOf('month').format('YYYY-MM-DD'))
              timeUserTomo = new Date(moment().add(1, 'month').startOf('month').format('YYYY-MM-DD'))
            }
            const cek = await activity.findAll({
              where: {
                [Op.and]: [
                  { kode_plant: kode },
                  { tipe: 'kasir' },
                  { jenis_dokumen: tipeValue }
                ],
                createdAt: {
                  [Op.lt]: timeUserTomo,
                  [Op.gt]: timeUser
                }
              }
            })
            if (cek.length > 0) {
              return response(res, 'list dokumen', { results, pageInfo, cek })
            } else {
              if (tipeValue === 'daily') {
                const now = new Date(moment().clone().startOf('month').format('YYYY-MM-DD'))
                const tomo = new Date(moment().add(1, 'month').startOf('month').format('YYYY-MM-DD'))
                const find = await activity.findAll({
                  where: {
                    [Op.and]: [
                      { kode_plant: kode },
                      { tipe: 'kasir' },
                      { jenis_dokumen: tipeValue }
                    ],
                    createdAt: {
                      [Op.lt]: tomo,
                      [Op.gt]: now
                    }
                  }
                })
                if (find) {
                  const temp = []
                  find.map(item => {
                    return temp.push(item.id)
                  })
                  for (let i = 0; i < find.length; i++) {
                    const send = {
                      access: 'lock'
                    }
                    const change = await activity.findByPk(temp[i])
                    if (change) {
                      await change.update(send)
                    }
                  }
                  const data = {
                    kode_plant: kode,
                    status: 'Belum Upload',
                    documentDate: new Date(moment().subtract(1, 'days')),
                    access: 'unlock',
                    jenis_dokumen: tipeValue === 'daily' ? 'daily' : 'monthly',
                    tipe: 'kasir'
                  }
                  const create = await activity.create(data)
                  if (create) {
                    return response(res, 'list dokumen', { results, pageInfo })
                  } else {
                    return response(res, 'failed to get dokumen', {}, 404, false)
                  }
                } else {
                  const data = {
                    kode_plant: kode,
                    status: 'Belum Upload',
                    documentDate: new Date(moment().subtract(1, 'days')),
                    access: 'unlock',
                    jenis_dokumen: tipeValue === 'daily' ? 'daily' : 'monthly',
                    tipe: 'kasir'
                  }
                  const create = await activity.create(data)
                  if (create) {
                    return response(res, 'list dokumen', { results, pageInfo, cek })
                  } else {
                    return response(res, 'failed to get dokumen', {}, 404, false)
                  }
                }
              } else {
                const data = {
                  kode_plant: kode,
                  status: 'Belum Upload',
                  documentDate: new Date(moment().subtract(1, 'days')),
                  access: 'unlock',
                  jenis_dokumen: tipeValue === 'daily' ? 'daily' : 'monthly',
                  tipe: 'kasir'
                }
                const create = await activity.create(data)
                if (create) {
                  return response(res, 'list dokumen', { results, pageInfo, cek })
                } else {
                  return response(res, 'failed to get dokumen', {}, 404, false)
                }
              }
            }
          } else {
            return response(res, 'failed to get dokumen', {}, 404, false)
          }
        } else {
          return response(res, 'user tidak terhubung dengan depo manapun', {}, 404, false)
        }
      } else if (level === 3) {
        const name = req.user.name
        const results = await pic.findAndCountAll({
          where: {
            pic: name
          },
          limit: limit,
          offset: (page - 1) * limit,
          include: [
            {
              model: depo,
              as: 'depo',
              where: {
                [Op.or]: [
                  { kode_plant: { [Op.like]: `%${findValue}%` } },
                  { nama_depo: { [Op.like]: `%${findValue}%` } },
                  { home_town: { [Op.like]: `%${findValue}%` } }
                ]
              }
            }
          ]
        })
        const pageInfo = pagination('/dashboard/get', req.query, page, limit, results.count)
        if (results) {
          const depos = []
          results.rows.map(x => {
            return (
              depos.push(x.depo)
            )
          })
          if (depos.length > 0) {
            const sa = []
            const kasir = []
            const all = []
            if (tipeValue === 'monthly') {
              now = new Date(moment(timeValue).startOf('month').format('YYYY-MM-DD'))
              tomo = new Date(moment(timeValue).endOf('month').format('YYYY-MM-DD'))
            }
            for (let i = 0; i < depos.length; i++) {
              const result = await depo.findAndCountAll({
                where: {
                  kode_plant: depos[i].kode_plant
                },
                include: [
                  {
                    model: activity,
                    as: 'active',
                    where: {
                      [Op.and]: [
                        { kode_plant: depos[i].kode_plant },
                        { tipe: 'sa' },
                        { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } }
                      ],
                      createdAt: {
                        [Op.lt]: tomo,
                        [Op.gt]: now
                      }
                    },
                    limit: 1,
                    include: [
                      {
                        model: Path,
                        as: 'doc',
                        limit: 50
                      }
                    ]
                  },
                  {
                    model: documents,
                    as: 'dokumen',
                    where: {
                      [Op.or]: [
                        { nama_dokumen: { [Op.like]: `%${searchValue}%` } }
                      ],
                      [Op.and]: [
                        { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } },
                        { uploadedBy: 'sa' }
                      ],
                      [Op.not]: { status: 'inactive' }
                    }
                  }
                ]
              })
              if (result) {
                sa.push(result.rows[0])
                if (result.rows[0] !== null && result.rows[0] !== undefined) {
                  all.push(result.rows[0])
                }
              }
            }
            for (let i = 0; i < depos.length; i++) {
              const result = await depo.findAndCountAll({
                where: {
                  kode_plant: depos[i].kode_plant
                },
                include: [
                  {
                    model: activity,
                    as: 'active',
                    where: {
                      [Op.and]: [
                        { kode_plant: depos[i].kode_plant },
                        { tipe: 'kasir' },
                        { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } }
                      ],
                      createdAt: {
                        [Op.lt]: tomo,
                        [Op.gt]: now
                      }
                    },
                    limit: 1,
                    include: [
                      {
                        model: Path,
                        as: 'doc',
                        limit: 50
                      }
                    ]
                  },
                  {
                    model: documents,
                    as: 'dokumen',
                    where: {
                      [Op.or]: [
                        { nama_dokumen: { [Op.like]: `%${searchValue}%` } }
                      ],
                      [Op.and]: [
                        { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } },
                        { uploadedBy: 'kasir' }
                      ],
                      [Op.not]: { status: 'inactive' }
                    }
                  }
                ]
              })
              if (result) {
                kasir.push(result.rows[0])
                if (result.rows[0] !== null && result.rows[0] !== undefined) {
                  all.push(result.rows[0])
                }
              }
            }
            if (sa.length > 0 || kasir.length > 0) {
              return response(res, 'list dokumen', { results, sa, kasir, all, pageInfo })
            } else {
              return response(res, 'list dokumen', { results, sa, kasir, all, pageInfo })
            }
          } else {
            return response(res, 'depo no found', {}, 404, false)
          }
        } else {
          return response(res, 'failed to get dokumen', {}, 404, false)
        }
      } else if (level === 2) {
        const name = req.user.name
        const results = await pic.findAndCountAll({
          where: {
            spv: name
          },
          limit: limit,
          offset: (page - 1) * limit,
          include: [
            {
              model: depo,
              as: 'depo',
              where: {
                [Op.or]: [
                  { kode_plant: { [Op.like]: `%${findValue}%` } },
                  { nama_depo: { [Op.like]: `%${findValue}%` } },
                  { home_town: { [Op.like]: `%${findValue}%` } }
                ]
              }
            }
          ]
        })
        const pageInfo = pagination('/dashboard/get', req.query, page, limit, results.count)
        if (results) {
          const depos = []
          results.rows.map(x => {
            return (
              depos.push(x)
            )
          })
          if (depos.length > 0) {
            const sa = []
            const kasir = []
            const all = []
            if (tipeValue === 'monthly') {
              now = new Date(moment(timeValue).startOf('month').format('YYYY-MM-DD'))
              tomo = new Date(moment(timeValue).endOf('month').format('YYYY-MM-DD'))
            }
            for (let i = 0; i < depos.length; i++) {
              const result = await depo.findAndCountAll({
                where: {
                  kode_plant: depos[i].kode_depo
                },
                include: [
                  {
                    model: activity,
                    as: 'active',
                    where: {
                      [Op.and]: [
                        { kode_plant: depos[i].kode_depo },
                        { tipe: 'sa' },
                        { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } }
                      ],
                      createdAt: {
                        [Op.lt]: tomo,
                        [Op.gt]: now
                      }
                    },
                    limit: 1,
                    include: [
                      {
                        model: Path,
                        as: 'doc',
                        limit: 50
                      }
                    ]
                  },
                  {
                    model: documents,
                    as: 'dokumen',
                    where: {
                      [Op.or]: [
                        { nama_dokumen: { [Op.like]: `%${searchValue}%` } }
                      ],
                      [Op.and]: [
                        { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } },
                        { uploadedBy: 'sa' }
                      ],
                      [Op.not]: { status: 'inactive' }
                    }
                  }
                ]
              })
              if (result) {
                sa.push(result.rows[0])
                if (result.rows[0] !== null && result.rows[0] !== undefined) {
                  all.push(result.rows[0])
                }
              }
            }
            for (let i = 0; i < depos.length; i++) {
              const result = await depo.findAndCountAll({
                where: {
                  kode_plant: depos[i].kode_depo
                },
                include: [
                  {
                    model: activity,
                    as: 'active',
                    where: {
                      [Op.and]: [
                        { kode_plant: depos[i].kode_depo },
                        { tipe: 'kasir' },
                        { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } }
                      ],
                      createdAt: {
                        [Op.lt]: tomo,
                        [Op.gt]: now
                      }
                    },
                    limit: 1,
                    include: [
                      {
                        model: Path,
                        as: 'doc',
                        limit: 50
                      }
                    ]
                  },
                  {
                    model: documents,
                    as: 'dokumen',
                    where: {
                      [Op.or]: [
                        { nama_dokumen: { [Op.like]: `%${searchValue}%` } }
                      ],
                      [Op.and]: [
                        { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } },
                        { uploadedBy: 'kasir' }
                      ],
                      [Op.not]: { status: 'inactive' }
                    }
                  }
                ]
              })
              if (result) {
                kasir.push(result.rows[0])
                if (result.rows[0] !== null && result.rows[0] !== undefined) {
                  all.push(result.rows[0])
                }
              }
            }
            if (sa.length > 0 || kasir.length > 0) {
              return response(res, 'list dokumen', { results, sa, kasir, all, pageInfo })
            } else {
              return response(res, 'list dokumen', { results, sa, kasir, all, pageInfo })
            }
          } else {
            return response(res, 'depo no found', {}, 404, false)
          }
        } else {
          return response(res, 'failed to get dokumen', {}, 404, false)
        }
      } else if (level === 1) {
        const results = await pic.findAndCountAll({
          where: {
            spv: { [Op.like]: '%%' }
          },
          limit: limit,
          offset: (page - 1) * limit,
          include: [
            {
              model: depo,
              as: 'depo',
              where: {
                [Op.or]: [
                  { kode_plant: { [Op.like]: `%${findValue}%` } },
                  { nama_depo: { [Op.like]: `%${findValue}%` } },
                  { home_town: { [Op.like]: `%${findValue}%` } }
                ]
              }
            }
          ]
        })
        const pageInfo = pagination('/dashboard/get', req.query, page, limit, results.count)
        if (results) {
          const depos = []
          results.rows.map(x => {
            return (
              depos.push(x)
            )
          })
          if (depos.length > 0) {
            const sa = []
            const kasir = []
            const all = []
            if (tipeValue === 'monthly') {
              now = new Date(moment(timeValue).startOf('month').format('YYYY-MM-DD'))
              tomo = new Date(moment(timeValue).endOf('month').format('YYYY-MM-DD'))
            }
            for (let i = 0; i < depos.length; i++) {
              const result = await depo.findAndCountAll({
                where: {
                  kode_plant: depos[i].kode_depo
                },
                include: [
                  {
                    model: activity,
                    as: 'active',
                    where: {
                      [Op.and]: [
                        { kode_plant: depos[i].kode_depo },
                        { tipe: 'sa' },
                        { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } }
                      ],
                      createdAt: {
                        [Op.lt]: tomo,
                        [Op.gt]: now
                      }
                    },
                    limit: 1,
                    include: [
                      {
                        model: Path,
                        as: 'doc',
                        limit: 50
                      }
                    ]
                  },
                  {
                    model: documents,
                    as: 'dokumen',
                    where: {
                      [Op.or]: [
                        { nama_dokumen: { [Op.like]: `%${searchValue}%` } }
                      ],
                      [Op.and]: [
                        { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } },
                        { uploadedBy: 'sa' }
                      ],
                      [Op.not]: { status: 'inactive' }
                    }
                  }
                ]
              })
              if (result) {
                sa.push(result.rows[0])
                if (result.rows[0] !== null && result.rows[0] !== undefined) {
                  all.push(result.rows[0])
                }
              }
            }
            for (let i = 0; i < depos.length; i++) {
              const result = await depo.findAndCountAll({
                where: {
                  kode_plant: depos[i].kode_depo
                },
                include: [
                  {
                    model: activity,
                    as: 'active',
                    where: {
                      [Op.and]: [
                        { kode_plant: depos[i].kode_depo },
                        { tipe: 'kasir' },
                        { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } }
                      ],
                      createdAt: {
                        [Op.lt]: tomo,
                        [Op.gt]: now
                      }
                    },
                    limit: 1,
                    include: [
                      {
                        model: Path,
                        as: 'doc',
                        limit: 50
                      }
                    ]
                  },
                  {
                    model: documents,
                    as: 'dokumen',
                    where: {
                      [Op.or]: [
                        { nama_dokumen: { [Op.like]: `%${searchValue}%` } }
                      ],
                      [Op.and]: [
                        { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } },
                        { uploadedBy: 'kasir' }
                      ],
                      [Op.not]: { status: 'inactive' }
                    }
                  }
                ]
              })
              if (result) {
                kasir.push(result.rows[0])
                if (result.rows[0] !== null && result.rows[0] !== undefined) {
                  all.push(result.rows[0])
                }
              }
            }
            if (sa.length > 0 || kasir.length > 0) {
              return response(res, 'list dokumen', { results, sa, kasir, all, pageInfo })
            } else {
              return response(res, 'list dokumen', { results, sa, kasir, all, pageInfo })
            }
          } else {
            return response(res, 'depo no found', {}, 404, false)
          }
        } else {
          return response(res, 'failed to get dokumen', {}, 404, false)
        }
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  getDashboard: async (req, res) => {
    try {
      // Extract and normalize query parameters
      const extractParam = (param) => {
        if (typeof param === 'object') return Object.values(param)[0]
        return param || ''
      }

      const {
        limit = 10,
        page = 1,
        search = '',
        sort = 'id',
        typeSort = 'DESC',
        time = '',
        tipe = 'daily',
        find = '',
        statusUpload = 'all'
      } = {
        limit: parseInt(req.query.limit) || 10,
        page: parseInt(req.query.page) || 1,
        search: extractParam(req.query.search),
        sort: extractParam(req.query.sort) || 'id',
        typeSort: extractParam(req.query.typeSort) || 'DESC',
        time: extractParam(req.query.time),
        tipe: extractParam(req.query.tipe) || 'daily',
        find: extractParam(req.query.find),
        statusUpload: extractParam(req.query.statusUpload) || 'all'
      }

      const { level, kode, name } = req.user

      // Handle level 4 and 5 (SA dan Kasir)
      if (level === 4 || level === 5) {
        // const offset = (page - 1) * limit
        const userType = level === 4 ? 'sa' : 'kasir'

        const depoQuery = `
          SELECT status_depo, kode_plant 
          FROM depos
          WHERE kode_plant = ? OR kode_depo = ?
          LIMIT 1
        `
        const [depoResult] = await sequelize.query(depoQuery, {
          replacements: [kode, kode],
          type: QueryTypes.SELECT
        })

        if (!depoResult) {
          return response(res, 'user tidak terhubung dengan depo manapun', {}, 404, false)
        }

        const documentsQuery = `
          SELECT 
            id, nama_dokumen, jenis_dokumen, postDokumen, divisi,
            status_depo, uploadedBy, status, access, createdAt, updatedAt
          FROM documents
          WHERE nama_dokumen LIKE ?
            AND (access LIKE ? OR access IS NULL)
            AND status_depo = ?
            AND uploadedBy = ?
            AND jenis_dokumen LIKE ?
            AND status != 'inactive'
          ORDER BY ${sort} ${typeSort}
        `
        const documents = await sequelize.query(documentsQuery, {
          replacements: [`%${search}%`, `%${kode}%`, depoResult.status_depo, userType, `%${tipe}%`],
          type: QueryTypes.SELECT
        })

        const countQuery = `
          SELECT COUNT(*) as total FROM documents
          WHERE nama_dokumen LIKE ?
            AND (access LIKE ? OR access IS NULL)
            AND status_depo = ?
            AND uploadedBy = ?
            AND jenis_dokumen LIKE ?
            AND status != 'inactive'
        `
        const [countResult] = await sequelize.query(countQuery, {
          replacements: [`%${search}%`, `%${kode}%`, depoResult.status_depo, userType, `%${tipe}%`],
          type: QueryTypes.SELECT
        })

        const results = {
          rows: documents,
          count: countResult.total
        }

        const pageInfo = pagination('/dokumen/get', req.query, page, limit, results.count)

        let timeUser = new Date(moment().format('YYYY-MM-DD 00:00'))
        let timeUserTomo = new Date(moment().add(1, 'days').format('YYYY-MM-DD 00:00'))

        if (tipe === 'monthly') {
          timeUser = new Date(moment().startOf('month').format('YYYY-MM-DD'))
          timeUserTomo = new Date(moment().add(1, 'month').startOf('month').format('YYYY-MM-DD'))
        }

        const activityQuery = `
          SELECT * FROM activities
          WHERE kode_plant = ?
            AND tipe = ?
            AND jenis_dokumen = ?
            AND createdAt > ?
            AND createdAt < ?
        `
        let cek = await sequelize.query(activityQuery, {
          replacements: [kode, userType, tipe, timeUser, timeUserTomo],
          type: QueryTypes.SELECT
        })

        if (cek.length === 0) {
          if (tipe === 'daily') {
            const monthStart = new Date(moment().startOf('month').format('YYYY-MM-DD'))
            const monthEnd = new Date(moment().add(1, 'month').startOf('month').format('YYYY-MM-DD'))

            const findQuery = `
              SELECT id FROM activities
              WHERE kode_plant = ?
                AND tipe = ?
                AND jenis_dokumen = ?
                AND createdAt > ?
                AND createdAt < ?
            `
            const findResults = await sequelize.query(findQuery, {
              replacements: [kode, userType, tipe, monthStart, monthEnd],
              type: QueryTypes.SELECT
            })

            if (findResults.length > 0) {
              const ids = findResults.map(x => x.id)
              await sequelize.query('UPDATE activities SET access = \'lock\' WHERE id IN (?)', {
                replacements: [ids]
              })
            }
          }

          const insertQuery = `
            INSERT INTO activities (kode_plant, status, documentDate, access, jenis_dokumen, tipe, createdAt, updatedAt)
            VALUES (?, 'Belum Upload', ?, 'unlock', ?, ?, NOW(), NOW())
          `
          await sequelize.query(insertQuery, {
            replacements: [
              kode,
              new Date(moment().subtract(1, 'days').format('YYYY-MM-DD')),
              tipe,
              userType
            ]
          })

          cek = await sequelize.query(activityQuery, {
            replacements: [kode, userType, tipe, timeUser, timeUserTomo],
            type: QueryTypes.SELECT
          })
        }

        return response(res, 'list dokumen', { results, pageInfo, cek })
      }

      // Handle level 1, 2, 3 (Admin, Supervisor, PIC)
      if (level >= 1 && level <= 3) {
        console.time('⏱️ Total')

        let picQuery = ''
        let picReplacements = []

        // Query untuk mendapatkan SEMUA depo
        if (level === 3) {
          picQuery = `
            SELECT 
              p.id, p.pic, p.spv, p.divisi, p.kode_depo, p.nama_depo, p.status,
              p.createdAt as p_createdAt, p.updatedAt as p_updatedAt,
              d.id as depo_id, d.kode_depo as depo_kode_depo, d.nama_depo as depo_nama_depo,
              d.home_town, d.channel, d.distribution, d.status_depo, d.profit_center,
              d.kode_plant, d.kode_sap_1, d.kode_sap_2, d.nama_grom, d.nama_bm, d.nama_ass,
              d.nama_pic_1, d.nama_pic_2, d.nama_pic_3, d.nama_pic_4,
              d.createdAt as depo_createdAt, d.updatedAt as depo_updatedAt
            FROM pics p
            INNER JOIN depos d ON p.kode_depo = d.kode_plant
            WHERE p.pic = ?
              AND (d.kode_plant LIKE ? OR d.nama_depo LIKE ? OR d.home_town LIKE ?)
          `
          picReplacements = [name, `%${find}%`, `%${find}%`, `%${find}%`]
        } else if (level === 2) {
          picQuery = `
            SELECT 
              p.id, p.pic, p.spv, p.divisi, p.kode_depo, p.nama_depo, p.status,
              p.createdAt as p_createdAt, p.updatedAt as p_updatedAt,
              d.id as depo_id, d.kode_depo as depo_kode_depo, d.nama_depo as depo_nama_depo,
              d.home_town, d.channel, d.distribution, d.status_depo, d.profit_center,
              d.kode_plant, d.kode_sap_1, d.kode_sap_2, d.nama_grom, d.nama_bm, d.nama_ass,
              d.nama_pic_1, d.nama_pic_2, d.nama_pic_3, d.nama_pic_4,
              d.createdAt as depo_createdAt, d.updatedAt as depo_updatedAt
            FROM pics p
            INNER JOIN depos d ON p.kode_depo = d.kode_plant
            WHERE p.spv = ?
              AND (d.kode_plant LIKE ? OR d.nama_depo LIKE ? OR d.home_town LIKE ?)
          `
          picReplacements = [name, `%${find}%`, `%${find}%`, `%${find}%`]
        } else {
          picQuery = `
            SELECT 
              p.id, p.pic, p.spv, p.divisi, p.kode_depo, p.nama_depo, p.status,
              p.createdAt as p_createdAt, p.updatedAt as p_updatedAt,
              d.id as depo_id, d.kode_depo as depo_kode_depo, d.nama_depo as depo_nama_depo,
              d.home_town, d.channel, d.distribution, d.status_depo, d.profit_center,
              d.kode_plant, d.kode_sap_1, d.kode_sap_2, d.nama_grom, d.nama_bm, d.nama_ass,
              d.nama_pic_1, d.nama_pic_2, d.nama_pic_3, d.nama_pic_4,
              d.createdAt as depo_createdAt, d.updatedAt as depo_updatedAt
            FROM pics p
            INNER JOIN depos d ON p.kode_depo = d.kode_plant
            WHERE p.spv LIKE '%%'
              AND (d.kode_plant LIKE ? OR d.nama_depo LIKE ? OR d.home_town LIKE ?)
          `
          picReplacements = [`%${find}%`, `%${find}%`, `%${find}%`]
        }

        console.time('1️⃣ Pic query')
        const picResults = await sequelize.query(picQuery, {
          replacements: picReplacements,
          type: QueryTypes.SELECT
        })
        console.timeEnd('1️⃣ Pic query')

        const transformedPics = picResults.map(row => ({
          id: row.id,
          pic: row.pic,
          spv: row.spv,
          divisi: row.divisi,
          kode_depo: row.kode_depo,
          nama_depo: row.nama_depo,
          status: row.status,
          createdAt: row.p_createdAt,
          updatedAt: row.p_updatedAt,
          depo: {
            id: row.depo_id,
            kode_depo: row.depo_kode_depo,
            nama_depo: row.depo_nama_depo,
            home_town: row.home_town,
            channel: row.channel,
            distribution: row.distribution,
            status_depo: row.status_depo,
            profit_center: row.profit_center,
            kode_plant: row.kode_plant,
            kode_sap_1: row.kode_sap_1,
            kode_sap_2: row.kode_sap_2,
            nama_grom: row.nama_grom,
            nama_bm: row.nama_bm,
            nama_ass: row.nama_ass,
            nama_pic_1: row.nama_pic_1,
            nama_pic_2: row.nama_pic_2,
            nama_pic_3: row.nama_pic_3,
            nama_pic_4: row.nama_pic_4,
            createdAt: row.depo_createdAt,
            updatedAt: row.depo_updatedAt
          }
        }))

        if (!transformedPics.length) {
          return response(res, 'depo no found', {}, 404, false)
        }

        const depos = level === 3 ? transformedPics.map(x => x.depo) : transformedPics
        const kodeDepos = level === 3
          ? depos.map(x => x.kode_plant)
          : depos.map(x => x.kode_depo)

        let now = time ? moment(time).startOf('day').toDate() : moment().startOf('day').toDate()
        let tomo = time ? moment(time).add(1, 'days').startOf('day').toDate() : moment().add(1, 'days').startOf('day').toDate()

        if (tipe === 'monthly') {
          now = time ? moment(time).startOf('month').toDate() : moment().startOf('month').toDate()
          tomo = time ? moment(time).endOf('month').toDate() : moment().endOf('month').toDate()
        }

        const uniqueKodeDepos = [...new Set(kodeDepos)]
        console.log(`📊 Total unique depos: ${uniqueKodeDepos.length}`)

        console.time('2️⃣ Main queries')

        // QUERY 1: Ambil activity IDs yang punya paths (uploaded)
        const uploadedActivityQuery = `
          SELECT DISTINCT a.id, a.kode_plant, a.tipe
          FROM activities a
          INNER JOIN Paths p ON a.id = p.activityId
          WHERE a.kode_plant IN (?)
            AND a.tipe IN ('sa', 'kasir')
            AND a.jenis_dokumen LIKE ?
            AND a.createdAt > ?
            AND a.createdAt < ?
        `

        // QUERY 2: Ambil semua depos data (sudah ada dari pic query, tapi kita perlu struktur clean)
        const depoDataQuery = `
          SELECT 
            d.id, d.kode_depo, d.nama_depo, d.home_town, d.channel, d.distribution,
            d.status_depo, d.profit_center, d.kode_plant, d.kode_sap_1, d.kode_sap_2,
            d.nama_grom, d.nama_bm, d.nama_ass, d.nama_pic_1, d.nama_pic_2,
            d.nama_pic_3, d.nama_pic_4, d.createdAt, d.updatedAt
          FROM depos d
          WHERE d.kode_plant IN (?)
        `

        const [uploadedActivities, allDepos] = await Promise.all([
          sequelize.query(uploadedActivityQuery, {
            replacements: [uniqueKodeDepos, `%${tipe}%`, now, tomo],
            type: QueryTypes.SELECT
          }),
          sequelize.query(depoDataQuery, {
            replacements: [uniqueKodeDepos],
            type: QueryTypes.SELECT
          })
        ])

        console.timeEnd('2️⃣ Main queries')
        console.log(`✅ Uploaded activities found: ${uploadedActivities.length}`)

        console.time('3️⃣ Build uploaded set')
        // Build Set untuk lookup cepat uploaded depos
        const uploadedDepoKeys = new Set()
        uploadedActivities.forEach(act => {
          uploadedDepoKeys.add(`${act.kode_plant}_${act.tipe}`)
        })
        console.timeEnd('3️⃣ Build uploaded set')

        console.time('4️⃣ Conditional queries')
        // Conditional queries berdasarkan statusUpload
        let detailedActivities = []
        let allPaths = []
        let allDocuments = []

        if (statusUpload === 'uploaded' || statusUpload === 'all') {
          // Ambil detail activities dan paths hanya untuk yang uploaded
          const activityIds = uploadedActivities.map(a => a.id)

          if (activityIds.length > 0) {
            const detailActivityQuery = `
              SELECT 
                a.id, a.kode_plant, a.progress, a.documentDate, a.status,
                a.access, a.jenis_dokumen, a.tipe, a.createdAt, a.updatedAt
              FROM activities a
              WHERE a.id IN (?)
            `

            const pathsQuery = `
              SELECT 
                p.id, p.dokumen, p.activityId, p.kode_depo, p.alasan,
                p.status_dokumen, p.path, p.createdAt, p.updatedAt
              FROM Paths p
              WHERE p.activityId IN (?)
              LIMIT 5000
            `

            const documentsQuery = `
              SELECT 
                doc.id, doc.nama_dokumen, doc.jenis_dokumen, doc.postDokumen,
                doc.divisi, doc.status_depo, doc.uploadedBy, doc.status,
                doc.access, doc.createdAt, doc.updatedAt
              FROM documents doc
              WHERE doc.nama_dokumen LIKE ?
                AND doc.jenis_dokumen LIKE ?
                AND doc.uploadedBy IN ('sa', 'kasir')
                AND doc.status != 'inactive'
            `

            const results = await Promise.all([
              sequelize.query(detailActivityQuery, {
                replacements: [activityIds],
                type: QueryTypes.SELECT
              }),
              sequelize.query(pathsQuery, {
                replacements: [activityIds],
                type: QueryTypes.SELECT
              }),
              sequelize.query(documentsQuery, {
                replacements: [`%${search}%`, `%${tipe}%`],
                type: QueryTypes.SELECT
              })
            ])

            detailedActivities = results[0]
            allPaths = results[1]
            allDocuments = results[2]
          }
        } else {
          // Untuk not uploaded, hanya perlu documents
          const documentsQuery = `
            SELECT 
              doc.id, doc.nama_dokumen, doc.jenis_dokumen, doc.postDokumen,
              doc.divisi, doc.status_depo, doc.uploadedBy, doc.status,
              doc.access, doc.createdAt, doc.updatedAt
            FROM documents doc
            WHERE doc.nama_dokumen LIKE ?
              AND doc.jenis_dokumen LIKE ?
              AND doc.uploadedBy IN ('sa', 'kasir')
              AND doc.status != 'inactive'
          `

          allDocuments = await sequelize.query(documentsQuery, {
            replacements: [`%${search}%`, `%${tipe}%`],
            type: QueryTypes.SELECT
          })
        }

        console.timeEnd('4️⃣ Conditional queries')

        console.time('5️⃣ Build maps')
        // Build lookup maps
        const depoMap = {}
        allDepos.forEach(d => {
          depoMap[d.kode_plant] = d
        })

        const activityMap = {}
        detailedActivities.forEach(a => {
          const key = `${a.kode_plant}_${a.tipe}`
          activityMap[key] = a
        })

        const pathMap = {}
        allPaths.forEach(p => {
          if (!pathMap[p.activityId]) {
            pathMap[p.activityId] = []
          }
          pathMap[p.activityId].push(p)
        })

        const documentMap = {}
        allDocuments.forEach(doc => {
          const key = `${doc.status_depo}_${doc.uploadedBy}`
          if (!documentMap[key]) {
            documentMap[key] = []
          }
          documentMap[key].push(doc)
        })
        console.timeEnd('5️⃣ Build maps')

        console.time('6️⃣ Build final data')
        const finalData = []

        if (statusUpload === 'uploaded') {
          // Hanya build data uploaded
          uniqueKodeDepos.forEach(kodeDepo => {
            const depoData = depoMap[kodeDepo]
            if (!depoData) return

            const saKey = `${kodeDepo}_sa`
            const kasirKey = `${kodeDepo}_kasir`

            if (uploadedDepoKeys.has(saKey)) {
              const saActivity = activityMap[saKey]
              const saPaths = saActivity ? pathMap[saActivity.id] || [] : []
              const saDocuments = documentMap[`${depoData.status_depo}_sa`] || []

              finalData.push({
                ...depoData,
                userType: 'sa',
                active: saActivity ? [{ ...saActivity, doc: saPaths }] : [],
                dokumen: saDocuments
              })
            }

            if (uploadedDepoKeys.has(kasirKey)) {
              const kasirActivity = activityMap[kasirKey]
              const kasirPaths = kasirActivity ? pathMap[kasirActivity.id] || [] : []
              const kasirDocuments = documentMap[`${depoData.status_depo}_kasir`] || []

              finalData.push({
                ...depoData,
                userType: 'kasir',
                active: kasirActivity ? [{ ...kasirActivity, doc: kasirPaths }] : [],
                dokumen: kasirDocuments
              })
            }
          })
        } else if (statusUpload === 'not uploaded') {
          // Hanya build data not uploaded
          uniqueKodeDepos.forEach(kodeDepo => {
            const depoData = depoMap[kodeDepo]
            if (!depoData) return

            const saKey = `${kodeDepo}_sa`
            const kasirKey = `${kodeDepo}_kasir`

            if (!uploadedDepoKeys.has(saKey)) {
              const saDocuments = documentMap[`${depoData.status_depo}_sa`] || []
              finalData.push({
                ...depoData,
                userType: 'sa',
                active: [],
                dokumen: saDocuments
              })
            }

            if (!uploadedDepoKeys.has(kasirKey)) {
              const kasirDocuments = documentMap[`${depoData.status_depo}_kasir`] || []
              finalData.push({
                ...depoData,
                userType: 'kasir',
                active: [],
                dokumen: kasirDocuments
              })
            }
          })
        } else {
          // 'all' - build uploaded + not uploaded
          uniqueKodeDepos.forEach(kodeDepo => {
            const depoData = depoMap[kodeDepo]
            if (!depoData) return

            const saKey = `${kodeDepo}_sa`
            const kasirKey = `${kodeDepo}_kasir`

            // SA
            const saUploaded = uploadedDepoKeys.has(saKey)
            if (saUploaded) {
              const saActivity = activityMap[saKey]
              const saPaths = saActivity ? pathMap[saActivity.id] || [] : []
              const saDocuments = documentMap[`${depoData.status_depo}_sa`] || []

              finalData.push({
                ...depoData,
                userType: 'sa',
                active: saActivity ? [{ ...saActivity, doc: saPaths }] : [],
                dokumen: saDocuments
              })
            } else {
              const saDocuments = documentMap[`${depoData.status_depo}_sa`] || []
              finalData.push({
                ...depoData,
                userType: 'sa',
                active: [],
                dokumen: saDocuments
              })
            }

            // Kasir
            const kasirUploaded = uploadedDepoKeys.has(kasirKey)
            if (kasirUploaded) {
              const kasirActivity = activityMap[kasirKey]
              const kasirPaths = kasirActivity ? pathMap[kasirActivity.id] || [] : []
              const kasirDocuments = documentMap[`${depoData.status_depo}_kasir`] || []

              finalData.push({
                ...depoData,
                userType: 'kasir',
                active: kasirActivity ? [{ ...kasirActivity, doc: kasirPaths }] : [],
                dokumen: kasirDocuments
              })
            } else {
              const kasirDocuments = documentMap[`${depoData.status_depo}_kasir`] || []
              finalData.push({
                ...depoData,
                userType: 'kasir',
                active: [],
                dokumen: kasirDocuments
              })
            }
          })
        }

        const totalCount = finalData.length
        console.log(`📦 Final data count: ${totalCount}`)
        console.timeEnd('6️⃣ Build final data')

        console.time('7️⃣ Pagination')
        // Apply pagination
        const offset = (page - 1) * limit
        const paginatedData = finalData.slice(offset, offset + limit)

        // Separate SA dan Kasir
        const sa = paginatedData.filter(d => d.userType === 'sa')
        const kasir = paginatedData.filter(d => d.userType === 'kasir')
        console.timeEnd('7️⃣ Pagination')

        const updatedPageInfo = pagination('/dashboard/get', req.query, page, limit, totalCount)

        const results = {
          rows: transformedPics,
          count: transformedPics.length
        }

        console.timeEnd('⏱️ Total')

        return response(res, 'list dokumen', {
          results,
          sa,
          kasir,
          all: paginatedData,
          pageInfo: updatedPageInfo
        })
      }
    } catch (error) {
      console.error('Dashboard error:', error)
      return response(res, error.message, {}, 500, false)
    }
  },
  uploadDocument: async (req, res) => {
    const id = req.params.id
    const idAct = req.params.idAct
    const level = req.user.level
    const kode = req.user.kode
    let time = req.params.time
    if (time !== undefined) {
      time = req.params.time
    } else {
      time = moment().utc().format('YYYY-MM-DD')
    }
    uploadHelper(req, res, async function (err) {
      try {
        if (err instanceof multer.MulterError) {
          if (err.code === 'LIMIT_UNEXPECTED_FILE' && req.files.length === 0) {
            console.log(err.code === 'LIMIT_UNEXPECTED_FILE' && req.files.length > 0)
            return response(res, 'fieldname doesnt match', {}, 500, false)
          }
          return response(res, err.message, {}, 500, false)
        } else if (err) {
          return response(res, err.message, {}, 401, false)
        } else {
          const dokumen = `assets/documents/${req.file.filename}`
          if (level === 5) {
            const result = await documents.findByPk(id)
            if (result) {
              const active = await activity.findByPk(idAct)
              if (active.access === 'lock') {
                return response(res, 'Dokumen ini sudah di lock, hubungi spv untuk mengizinkan upload dokumen', {}, 404, false)
              } else {
                if (result.jenis_dokumen === 'daily') {
                  const time = await date_clossing.findOne({
                    where: {
                      jenis: 'daily'
                    }
                  })
                  if (moment().format('HH:mm') < moment(time.time).format('HH:mm')) {
                    const send = { dokumen: result.nama_dokumen, activityId: idAct, path: dokumen, kode_depo: kode, status_dokumen: 1 }
                    const upload = await Path.create(send)
                    if (upload) {
                      const data = {
                        kode_plant: kode,
                        reject: 'false',
                        upload: 'true',
                        activityId: idAct,
                        pathId: upload.id,
                        tipe: 'kasir'
                      }
                      const result = await notif.create(data)
                      if (result) {
                        return response(res, 'successfully upload dokumen', { upload })
                      } else {
                        return response(res, 'failed to upload dokumen', {}, 404, false)
                      }
                    } else {
                      return response(res, 'failed to upload dokumen', {}, 404, false)
                    }
                  } else {
                    const send = { dokumen: result.nama_dokumen, activityId: idAct, path: dokumen, kode_depo: kode, status_dokumen: 4 }
                    const upload = await Path.create(send)
                    if (upload) {
                      const data = {
                        kode_plant: kode,
                        reject: 'false',
                        upload: 'true',
                        activityId: idAct,
                        pathId: upload.id,
                        tipe: 'kasir'
                      }
                      const result = await notif.create(data)
                      if (result) {
                        return response(res, 'successfully upload dokumen', { upload })
                      } else {
                        return response(res, 'failed to upload dokumen', {}, 404, false)
                      }
                    } else {
                      return response(res, 'failed to upload dokumen', {}, 404, false)
                    }
                  }
                } else if (result.jenis_dokumen === 'monthly') {
                  const time = await date_clossing.findOne({
                    where: {
                      jenis: 'monthly'
                    }
                  })
                  if (moment().format('LL') < moment(time.day).format('LL')) {
                    const send = { dokumen: result.nama_dokumen, activityId: idAct, path: dokumen, kode_depo: kode, status_dokumen: 1 }
                    const upload = await Path.create(send)
                    if (upload) {
                      const data = {
                        kode_plant: kode,
                        reject: 'false',
                        upload: 'true',
                        activityId: idAct,
                        pathId: upload.id,
                        tipe: 'kasir'
                      }
                      const result = await notif.create(data)
                      if (result) {
                        return response(res, 'successfully upload dokumen', { upload })
                      } else {
                        return response(res, 'failed to upload dokumen', {}, 404, false)
                      }
                    } else {
                      return response(res, 'failed to upload dokumen', {}, 404, false)
                    }
                  } else {
                    const send = { dokumen: result.nama_dokumen, activityId: idAct, path: dokumen, kode_depo: kode, status_dokumen: 4 }
                    const upload = await Path.create(send)
                    if (upload) {
                      const data = {
                        kode_plant: kode,
                        reject: 'false',
                        upload: 'true',
                        activityId: idAct,
                        pathId: upload.id,
                        tipe: 'kasir'
                      }
                      const result = await notif.create(data)
                      if (result) {
                        return response(res, 'successfully upload dokumen', { upload })
                      } else {
                        return response(res, 'failed to upload dokumen', {}, 404, false)
                      }
                    } else {
                      return response(res, 'failed to upload dokumen', {}, 404, false)
                    }
                  }
                }
              }
            } else {
              return response(res, 'failed to upload dokumen', {}, 404, false)
            }
          } else if (level === 4) {
            const result = await documents.findByPk(id)
            if (result) {
              const active = await activity.findByPk(idAct)
              if (active.access === 'lock') {
                return response(res, 'Dokumen ini sudah di lock, hubungi spv untuk mengizinkan upload dokumen', {}, 404, false)
              } else {
                if (result.jenis_dokumen === 'daily') {
                  const time = await date_clossing.findOne({
                    where: {
                      jenis: 'daily'
                    }
                  })
                  if (moment().format('HH:mm') < moment(time.time).format('HH:mm')) {
                    const send = { dokumen: result.nama_dokumen, activityId: idAct, path: dokumen, kode_depo: kode, status_dokumen: 1 }
                    const upload = await Path.create(send)
                    if (upload) {
                      const data = {
                        kode_plant: kode,
                        reject: 'false',
                        upload: 'true',
                        activityId: idAct,
                        pathId: upload.id,
                        tipe: 'sa'
                      }
                      const result = await notif.create(data)
                      if (result) {
                        return response(res, 'successfully upload dokumen', { upload })
                      } else {
                        return response(res, 'failed to upload dokumen', {}, 404, false)
                      }
                    } else {
                      return response(res, 'failed to upload dokumen', {}, 404, false)
                    }
                  } else {
                    const send = { dokumen: result.nama_dokumen, activityId: idAct, path: dokumen, kode_depo: kode, status_dokumen: 4 }
                    const upload = await Path.create(send)
                    if (upload) {
                      const data = {
                        kode_plant: kode,
                        reject: 'false',
                        upload: 'true',
                        activityId: idAct,
                        pathId: upload.id,
                        tipe: 'sa'
                      }
                      const result = await notif.create(data)
                      if (result) {
                        return response(res, 'successfully upload dokumen', { upload })
                      } else {
                        return response(res, 'failed to upload dokumen', {}, 404, false)
                      }
                    } else {
                      return response(res, 'failed to upload dokumen', {}, 404, false)
                    }
                  }
                } else if (result.jenis_dokumen === 'monthly') {
                  const time = await date_clossing.findOne({
                    where: {
                      jenis: 'monthly'
                    }
                  })
                  if (moment().format('LL') < moment(time.day).format('LL')) {
                    const send = { dokumen: result.nama_dokumen, activityId: idAct, path: dokumen, kode_depo: kode, status_dokumen: 1 }
                    const upload = await Path.create(send)
                    if (upload) {
                      const data = {
                        kode_plant: kode,
                        reject: 'false',
                        upload: 'true',
                        activityId: idAct,
                        pathId: upload.id,
                        tipe: 'sa'
                      }
                      const result = await notif.create(data)
                      if (result) {
                        return response(res, 'successfully upload dokumen', { upload })
                      } else {
                        return response(res, 'failed to upload dokumen', {}, 404, false)
                      }
                    } else {
                      return response(res, 'failed to upload dokumen', {}, 404, false)
                    }
                  } else {
                    const send = { dokumen: result.nama_dokumen, activityId: idAct, path: dokumen, kode_depo: kode, status_dokumen: 4 }
                    const upload = await Path.create(send)
                    if (upload) {
                      const data = {
                        kode_plant: kode,
                        reject: 'false',
                        upload: 'true',
                        activityId: idAct,
                        pathId: upload.id,
                        tipe: 'sa'
                      }
                      const result = await notif.create(data)
                      if (result) {
                        return response(res, 'successfully upload dokumen', { upload })
                      } else {
                        return response(res, 'failed to upload dokumen', {}, 404, false)
                      }
                    } else {
                      return response(res, 'failed to upload dokumen', {}, 404, false)
                    }
                  }
                }
              }
            } else {
              return response(res, 'failed to upload dokumen', {}, 404, false)
            }
          }
        }
      } catch (error) {
        return response(res, error.message, {}, 500, false)
      }
    })
  },
  editUploadDocument: async (req, res) => {
    const id = req.params.id
    const level = req.user.level
    const idAct = req.params.idAct
    uploadHelper(req, res, async function (err) {
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
        const dokumen = `assets/documents/${req.file.filename}`
        if (level === 4 || level === 5) {
          const valid = await Path.findByPk(id)
          if (valid) {
            const active = await activity.findByPk(idAct)
            if (active.access === 'lock') {
              return response(res, 'Dokumen ini sudah di lock, hubungi spv untuk mengizinkan upload dokumen', {}, 404, false)
            } else {
              const send = { path: dokumen, status_dokumen: 7 }
              await valid.update(send)
              return response(res, 'successfully upload dokumen', { send })
            }
          } else {
            return response(res, 'failed to edit upload dokumen', {}, 404, false)
          }
        } else {
          return response(res, "you can't edit dokumen", {}, 404, false)
        }
      } catch (error) {
        return response(res, error.message, {}, 500, false)
      }
    })
  },
  uploadDocumentMultiple: async (req, res) => {
    const idAct = req.params.idAct
    const level = req.user.level
    const kode = req.user.kode

    uploadHelperMultiple(req, res, async function (err) {
      try {
        if (err instanceof multer.MulterError) {
          if (err.code === 'LIMIT_UNEXPECTED_FILE' && req.files.length === 0) {
            return response(res, 'fieldname doesnt match', {}, 500, false)
          }
          return response(res, err.message, {}, 500, false)
        } else if (err) {
          return response(res, err.message, {}, 401, false)
        } else {
          // Validasi jika tidak ada file
          if (!req.files || req.files.length === 0) {
            return response(res, 'No files uploaded', {}, 400, false)
          }

          // Parse listId dari JSON string
          let listId
          try {
            listId = JSON.parse(req.body.listId)
          } catch (error) {
            return response(res, 'Invalid listId format', {}, 400, false)
          }

          // Validasi listId adalah array
          if (!Array.isArray(listId)) {
            return response(res, 'listId must be an array', {}, 400, false)
          }

          // Validasi jumlah file dan listId harus sama
          if (req.files.length !== listId.length) {
            return response(res, 'Number of files must match number of document IDs', {}, 400, false)
          }

          const active = await activity.findByPk(idAct)
          if (!active) {
            return response(res, 'Activity not found', {}, 404, false)
          }

          if (active.access === 'lock') {
            return response(res, 'Dokumen ini sudah di lock, hubungi spv untuk mengizinkan upload dokumen', {}, 404, false)
          }

          const tipe = level === 5 ? 'kasir' : 'sa'
          const uploadedFiles = []
          const failedUploads = []

          // Loop untuk upload multiple files dengan listId
          for (let i = 0; i < req.files.length; i++) {
            const file = req.files[i]
            const docId = listId[i]

            try {
              const result = await documents.findByPk(docId)
              const findPath = await Path.findByPk(docId)

              if (result) {
                // Determine status based on document type and time
                let statusDokumen = 1
                if (result.jenis_dokumen === 'daily') {
                  const time = await date_clossing.findOne({
                    where: { jenis: 'daily' }
                  })
                  if (moment().format('HH:mm') >= moment(time.time).format('HH:mm')) {
                    statusDokumen = 4
                  }
                } else if (result.jenis_dokumen === 'monthly') {
                  const time = await date_clossing.findOne({
                    where: { jenis: 'monthly' }
                  })
                  if (moment().format('LL') >= moment(time.day).format('LL')) {
                    statusDokumen = 4
                  }
                }

                const dokumen = `assets/documents/${file.filename}`
                const send = {
                  dokumen: result.nama_dokumen,
                  activityId: idAct,
                  path: dokumen,
                  kode_depo: kode,
                  status_dokumen: statusDokumen
                }

                const upload = await Path.create(send)

                if (upload) {
                  const data = {
                    kode_plant: kode,
                    reject: 'false',
                    upload: 'true',
                    activityId: idAct,
                    pathId: upload.id,
                    tipe: tipe
                  }
                  await notif.create(data)
                  uploadedFiles.push({
                    upload,
                    fileName: file.originalname,
                    docId: docId,
                    documentName: result.nama_dokumen
                  })
                } else {
                  failedUploads.push({
                    file: file.originalname,
                    reason: 'Failed to create path record',
                    docId: docId
                  })
                }
              } else if (findPath) {
                const active = await activity.findByPk(idAct)
                const dokumen = `assets/documents/${file.filename}`
                if (active.access === 'lock') {
                  return response(res, 'Dokumen ini sudah di lock, hubungi spv untuk mengizinkan upload dokumen', {}, 404, false)
                } else {
                  const send = { path: dokumen, status_dokumen: 7 }
                  await findPath.update(send)
                  uploadedFiles.push({
                    findPath,
                    fileName: file.originalname,
                    docId: docId,
                    documentName: findPath.document
                  })
                }
              } else {
                failedUploads.push({
                  file: file.originalname,
                  reason: 'Document not found',
                  docId: docId
                })
              }
            } catch (error) {
              failedUploads.push({
                file: file.originalname,
                reason: error.message,
                docId: docId
              })
            }
          }

          if (uploadedFiles.length > 0) {
            return response(res, 'Upload completed', {
              uploads: uploadedFiles,
              failed: failedUploads,
              summary: {
                total: req.files.length,
                success: uploadedFiles.length,
                failed: failedUploads.length
              }
            })
          } else {
            return response(res, 'All uploads failed', { failed: failedUploads }, 404, false)
          }
        }
      } catch (error) {
        return response(res, error.message, {}, 500, false)
      }
    })
  },
  getActivityOld: async (req, res) => {
    try {
      const level = req.user.level
      const kode = req.user.kode
      //   const time = moment().utc().format('YYYY-MM-DD')
      let { limit, page, search, sort, typeSort } = req.query
      let searchValue = ''
      let sortValue = ''
      let typeSortValue = ''
      if (typeof search === 'object') {
        searchValue = Object.values(search)[0]
      } else {
        searchValue = search || 'daily'
      }
      if (typeof sort === 'object') {
        sortValue = Object.values(sort)[0]
      } else {
        sortValue = sort || 'id'
      }
      if (typeof typeSort === 'object') {
        typeSortValue = Object.values(typeSort)[0]
      } else {
        typeSortValue = typeSort || 'DESC'
      }
      if (!limit) {
        limit = 30
      } else {
        limit = parseInt(limit)
      }
      if (!page) {
        page = 1
      } else {
        page = parseInt(page)
      }
      if (level === 4) {
        const result = await activity.findAndCountAll({
          where: {
            [Op.and]: [
              { kode_plant: kode },
              { tipe: 'sa' }
            ],
            jenis_dokumen: searchValue
          },
          include: [
            {
              model: Path,
              as: 'doc'
            }
          ],
          order: [[sortValue, typeSortValue]],
          limit: limit,
          offset: (page - 1) * limit
        })
        const pageInfo = pagination('/dashboard/activity', req.query, page, limit, result.count)
        if (result) {
          return response(res, 'list activity', { result, pageInfo })
        } else {
          return response(res, 'failed to get activity', {}, 404, false)
        }
      } else if (level === 5) {
        const result = await activity.findAndCountAll({
          where: {
            [Op.and]: [
              { kode_plant: kode },
              { tipe: 'kasir' }
            ],
            jenis_dokumen: searchValue
          },
          include: [
            {
              model: Path,
              as: 'doc'
            }
          ],
          order: [[sortValue, typeSortValue]],
          limit: limit,
          offset: (page - 1) * limit
        })
        const pageInfo = pagination('/dashboard/activity', req.query, page, limit, result.count)
        if (result) {
          return response(res, 'list activity', { result, pageInfo })
        } else {
          return response(res, 'failed to get activity', {}, 404, false)
        }
      } else if (level === 1 || level === 2 || level === 3) {
        const result = await activity.findAndCountAll({
          where: {
            jenis_dokumen: searchValue
          },
          include: [
            {
              model: Path,
              as: 'doc'
            }
          ],
          order: [[sortValue, typeSortValue]],
          limit: limit,
          offset: (page - 1) * limit
        })
        const pageInfo = pagination('/dashboard/activity', req.query, page, limit, result.count)
        if (result) {
          return response(res, 'list activity', { result, pageInfo })
        } else {
          return response(res, 'failed to get activity', {}, 404, false)
        }
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  getActivity: async (req, res) => {
    try {
      const level = req.user.level
      const kode = req.user.kode
      const { limit, page, search, sort, typeSort } = req.query

      // Parse query parameters
      const searchValue = typeof search === 'object' ? Object.values(search)[0] : (search || 'daily')
      const sortValue = typeof sort === 'object' ? Object.values(sort)[0] : (sort || 'id')
      const typeSortValue = typeof typeSort === 'object' ? Object.values(typeSort)[0] : (typeSort || 'DESC')
      const limitValue = limit ? parseInt(limit) : 30
      const pageValue = page ? parseInt(page) : 1
      const offset = (pageValue - 1) * limitValue

      // Validasi sortValue untuk keamanan (whitelist)
      const allowedSortFields = ['id', 'kode_plant', 'progress', 'documentDate', 'status', 'createdAt', 'updatedAt']
      const safeSortValue = allowedSortFields.includes(sortValue) ? sortValue : 'id'
      const safeTypeSortValue = ['ASC', 'DESC'].includes(typeSortValue.toUpperCase()) ? typeSortValue.toUpperCase() : 'DESC'

      // Build WHERE clause based on level
      const whereConditions = ['jenis_dokumen = :searchValue']
      const replacements = { searchValue, limit: limitValue, offset }

      if (level === 4) {
        whereConditions.push('kode_plant = :kode')
        whereConditions.push("tipe = 'sa'")
        replacements.kode = kode
      } else if (level === 5) {
        whereConditions.push('kode_plant = :kode')
        whereConditions.push("tipe = 'kasir'")
        replacements.kode = kode
      }

      const whereClause = whereConditions.join(' AND ')

      // Query untuk count
      const countQuery = `
        SELECT COUNT(*) as total
        FROM activities
        WHERE ${whereClause}
      `

      // Query untuk mendapatkan data
      const dataQuery = `
        SELECT 
          activities.id,
          activities.kode_plant,
          activities.progress,
          activities.documentDate,
          activities.status,
          activities.access,
          activities.jenis_dokumen,
          activities.tipe,
          activities.createdAt,
          activities.updatedAt
        FROM activities
        WHERE ${whereClause}
        ORDER BY activities.${safeSortValue} ${safeTypeSortValue}
        LIMIT :limit OFFSET :offset
      `

      // Query untuk get paths berdasarkan activity ids
      const [countResult, activityRows] = await Promise.all([
        sequelize.query(countQuery, {
          replacements,
          type: QueryTypes.SELECT
        }),
        sequelize.query(dataQuery, {
          replacements,
          type: QueryTypes.SELECT
        })
      ])

      // Jika ada activity, ambil paths-nya
      let pathRows = []
      if (activityRows.length > 0) {
        const activityIds = activityRows.map(row => row.id)

        const pathQuery = `
          SELECT 
            id,
            dokumen,
            activityId,
            kode_depo,
            alasan,
            status_dokumen,
            path,
            createdAt,
            updatedAt
          FROM Paths
          WHERE activityId IN (:activityIds)
        `

        pathRows = await sequelize.query(pathQuery, {
          replacements: { activityIds },
          type: QueryTypes.SELECT
        })
      }

      // Group paths by activityId
      const pathsByActivity = {}
      pathRows.forEach(path => {
        if (!pathsByActivity[path.activityId]) {
          pathsByActivity[path.activityId] = []
        }
        pathsByActivity[path.activityId].push({
          id: path.id,
          dokumen: path.dokumen,
          activityId: path.activityId,
          kode_depo: path.kode_depo,
          alasan: path.alasan,
          status_dokumen: path.status_dokumen,
          path: path.path,
          createdAt: path.createdAt,
          updatedAt: path.updatedAt
        })
      })

      // Combine activities with their paths
      const rows = activityRows.map(activity => ({
        id: activity.id,
        kode_plant: activity.kode_plant,
        progress: activity.progress,
        documentDate: activity.documentDate,
        status: activity.status,
        access: activity.access,
        jenis_dokumen: activity.jenis_dokumen,
        tipe: activity.tipe,
        createdAt: activity.createdAt,
        updatedAt: activity.updatedAt,
        doc: pathsByActivity[activity.id] || []
      }))

      const result = {
        count: countResult[0].total,
        rows: rows
      }

      const pageInfo = pagination('/dashboard/activity', req.query, pageValue, limitValue, result.count)

      if (result) {
        return response(res, 'list activity', { result, pageInfo })
      } else {
        return response(res, 'failed to get activity', {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  approveDocument: async (req, res) => {
    // try {
    const level = req.user.level
    // const ids = req.params.id
    const idAct = req.params.idAct
    const list = Object.values(req.body)
    if (level === 1 || level === 2 || level === 3) {
      const cek = []
      for (let i = 0; i < list.length; i++) {
        const result = await Path.findByPk(list[i])
        let approve = { status_dokumen: 3 }
        if (result) {
          if (result.status_dokumen === 3 || result.status_dokumen === 5) {
            const find = await notif.findOne({
              where: {
                [Op.or]: [
                  { upload: 'true' },
                  { reject: 'true' }
                ],
                pathId: list[i]
              }
            })
            if (find) {
              await find.destroy()
              cek.push(find)
            } else {
              cek.push(find)
            }
          } else if (result.status_dokumen === 4 || result.status_dokumen === 6) {
            approve = { status_dokumen: 5 }
            await result.update(approve)
            const act = await activity.findByPk(idAct)
            if (act) {
              const send = { progress: act.progress + 1 }
              await act.update(send)
              const find = await notif.findOne({
                where: {
                  [Op.or]: [
                    { upload: 'true' },
                    { reject: 'true' }
                  ],
                  pathId: list[i]
                }
              })
              if (find) {
                await find.destroy()
                cek.push(find)
              } else {
                cek.push(find)
              }
            } else {
              cek.push()
            }
          } else {
            approve = { status_dokumen: 3 }
            await result.update(approve)
            const act = await activity.findByPk(idAct)
            if (act) {
              const send = { progress: act.progress + 1 }
              await act.update(send)
              const find = await notif.findOne({
                where: {
                  [Op.or]: [
                    { upload: 'true' },
                    { reject: 'true' }
                  ],
                  pathId: list[i]
                }
              })
              if (find) {
                await find.destroy()
                cek.push(find)
              } else {
                cek.push(find)
              }
            } else {
              cek.push()
            }
          }
        } else {
          cek.push()
        }
      }
      if (cek.length > 0) {
        return response(res, 'succes approve dokumen')
      } else {
        return response(res, 'failed approve dokumen', {}, 404, false)
      }
    } else {
      return response(res, "you're not super administrator", {}, 404, false)
    }
    // } catch (error) {
    //   return response(res, error.message, {}, 500, false)
    // }
  },
  rejectDocument: async (req, res) => {
    // try {
    const level = req.user.level
    // const ids = req.params.id
    const idAct = req.params.idAct
    const list = Object.values(req.body)
    const alasan = list[0]
    if (level === 1 || level === 2 || level === 3) {
      const cek = []
      for (let i = 1; i < list.length; i++) {
        const result = await Path.findByPk(list[i])
        let send = {
          alasan: alasan,
          status_dokumen: 0
        }
        if (result) {
          if (result.status_dokumen === 3) {
            send = {
              alasan: alasan,
              status_dokumen: 0
            }
            await result.update(send)
            const act = await activity.findByPk(idAct)
            if (act) {
              const desc = { progress: act.progress - 1 }
              const update = await act.update(desc)
              if (update) {
                const data = {
                  kode_plant: act.kode_plant,
                  reject: 'true',
                  upload: 'false',
                  activityId: idAct,
                  pathId: list[i],
                  tipe: act.tipe
                }
                const find = await notif.findOne({
                  where: {
                    [Op.or]: [
                      { upload: 'true' },
                      { reject: 'true' }
                    ],
                    pathId: list[i]
                  }
                })
                if (find) {
                  await notif.create(data)
                  cek.push('success')
                } else {
                  await notif.create(data)
                  cek.push('success')
                }
              } else {
                cek.push()
              }
            } else {
              cek.push()
            }
          } else if (result.status_dokumen === 5) {
            send = {
              alasan: alasan,
              status_dokumen: 6
            }
            await result.update(send)
            const act = await activity.findByPk(idAct)
            if (act) {
              const desc = { progress: act.progress - 1 }
              const update = await act.update(desc)
              if (update) {
                const data = {
                  kode_plant: act.kode_plant,
                  reject: 'true',
                  upload: 'false',
                  activityId: idAct,
                  pathId: list[i],
                  tipe: act.tipe
                }
                const find = await notif.findOne({
                  where: {
                    [Op.or]: [
                      { upload: 'true' },
                      { reject: 'true' }
                    ],
                    pathId: list[i]
                  }
                })
                if (find) {
                  await notif.create(data)
                  cek.push('success')
                } else {
                  await notif.create(data)
                  cek.push('success')
                }
              } else {
                cek.push()
              }
            } else {
              cek.push()
            }
          } else if (result.status_dokumen === 6 || result.status_dokumen === 0) {
            cek.push('success')
          } else {
            send = {
              alasan: alasan,
              status_dokumen: 0
            }
            const update = await result.update(send)
            const act = await activity.findByPk(idAct)
            if (act && update) {
              const data = {
                kode_plant: act.kode_plant,
                reject: 'true',
                upload: 'false',
                activityId: idAct,
                pathId: list[i],
                tipe: act.tipe
              }
              const find = await notif.findOne({
                where: {
                  [Op.or]: [
                    { upload: 'true' },
                    { reject: 'true' }
                  ],
                  pathId: list[i]
                }
              })
              if (find) {
                await notif.create(data)
                cek.push('success')
              } else {
                await notif.create(data)
                cek.push('success')
              }
            } else {
              cek.push()
            }
          }
        } else {
          cek.push()
        }
      }
      if (cek.length > 0) {
        return response(res, 'succes approve dokumen')
      } else {
        return response(res, 'failed reject dokumen', {}, 404, false)
      }
    } else {
      return response(res, "you're not super administrator", {}, 404, false)
    }
    // } catch (error) {
    //   return response(res, error.message, {}, 500, false)
    // }
  },
  showDokumen: async (req, res) => {
    try {
      const id = req.params.id
      const result = await Path.findByPk(id)
      if (result) {
        const filePath = result.path
        fs.readFile(filePath, function (err, data) {
          if (err) {
            console.log(err)
          }
          res.contentType('application/pdf')
          res.send(data)
        })
      } else {
        return response(res, "can't show document", {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  sendMail: async (req, res) => {
    try {
      const level = req.user.level
      // const id = req.params.id
      const list = Object.values(req.body)
      if (level === 1 || level === 2 || level === 3) {
        const dok = await Path.findByPk(list[0])
        if (dok) {
          const act = await activity.findByPk(dok.activityId)
          if (act) {
            const result = await email.findOne({
              where: {
                [Op.and]: [
                  { kode_plant: act.kode_plant },
                  { tipe: act.tipe }
                ]
              }
            })
            if (result) {
              const find = await depo.findOne({
                where: {
                  [Op.or]: [
                    { kode_plant: result.kode_plant },
                    { kode_depo: result.kode_plant }
                  ]
                  // kode_plant: result.kode_plant
                }
              })
              if (find) {
                let tableTd = ''
                const cek = []
                for (let i = 0; i < list.length; i++) {
                  const findDoc = await Path.findByPk(list[i])
                  if (findDoc) {
                    const temp = findDoc.status_dokumen
                    const status = temp === 0 || temp === 6 ? 'Reject' : temp === 3 || temp === 5 ? 'Approve' : temp === null ? 'Belum upload' : 'Sudah upload'
                    const element = `
                        <tr>
                          <td>${i + 1}</td>
                          <td>${find.nama_depo}</td>
                          <td>${findDoc.dokumen}</td>
                          <td>${act.jenis_dokumen}</td>
                          <td>${moment(act.createdAt).subtract(1, 'day').format('DD-MM-YYYY')}</td>
                          <td>${moment(findDoc.createdAt).format('DD-MM-YYYY')}</td>
                          <td>${moment(findDoc.updatedAt).format('DD-MM-YYYY')}</td>
                          <td>${status}</td>
                          <td>${findDoc.alasan === null ? '' : findDoc.alasan}</td>
                        </tr>`
                    tableTd = tableTd + element
                    cek.push(1)
                  }
                }
                const mailOptions = {
                  from: 'noreply_acc@pinusmerahabadi.co.id',
                  replyTo: 'noreply_acc@pinusmerahabadi.co.id',
                  // to: 'insfopma@gmail.com',
                  // cc: 'fahmiazis797@gmail.com',
                  to: `${result.email_aos}`,
                  cc: `${result.email_sa_kasir}, ${result.email_ho_pic}`,
                  subject: 'Verifikasi Dokumen Report',
                  html: `<head>
                    <style type="text/css">
                      body {
                          display: flexbox;
                          flex-direction: column;
                      }
                      .tittle {
                          font-size: 15px;
                      }
                      .mar {
                          margin-bottom: 20px;
                      }
                      .mar1 {
                          margin-bottom: 10px;
                      }
                      .foot {
                          margin-top: 20px;
                          margin-bottom: 10px;
                      }
                      .foot1 {
                          margin-bottom: 50px;
                      }
                      .position {
                          display: flexbox;
                          flex-direction: row;
                          justify-content: left;
                          margin-top: 10px;
                      }
                      table {
                          font-family: "Lucida Sans Unicode", "Lucida Grande", "Segoe Ui";
                          font-size: 12px;
                      }
                      .demo-table {
                          border-collapse: collapse;
                          font-size: 13px;
                      }
                      .demo-table th, 
                      .demo-table td {
                          border-bottom: 1px solid #e1edff;
                          border-left: 1px solid #e1edff;
                          padding: 7px 17px;
                      }
                      .demo-table th, 
                      .demo-table td:last-child {
                          border-right: 1px solid #e1edff;
                      }
                      .demo-table td:first-child {
                          border-top: 1px solid #e1edff;
                      }
                      .demo-table td:last-child{
                          border-bottom: 0;
                      }
                      caption {
                          caption-side: top;
                          margin-bottom: 10px;
                      }
                      
                      /* Table Header */
                      .demo-table thead th {
                          background-color: #508abb;
                          color: #FFFFFF;
                          border-color: #6ea1cc !important;
                          text-transform: uppercase;
                      }
                      
                      /* Table Body */
                      .demo-table tbody td {
                          color: #353535;
                      }
                      
                      .demo-table tbody tr:nth-child(odd) td {
                          background-color: #f4fbff;
                      }
                      .demo-table tbody tr:hover th,
                      .demo-table tbody tr:hover td {
                          background-color: #ffffa2;
                          border-color: #ffff0f;
                          transition: all .2s;
                      }
                      .martit2 {
                          font-size: 15px;
                          margin-top: 20px;
                          margin-bottom: 20px;
                          font-weight: bold;
                      }
                      .martit {
                          font-size: 15px;
                          margin-bottom: 20px;
                          font-weight: bold;
                      }
                      .martit3 {
                          font-size: 15px;
                          margin-top: 10px;
                      }
                    </style>
                  </head>
                  <body>
                      <div class="martit">
                          Dear Bapak/Ibu AOS,
                      </div>
                      <div class="tittle mar1">
                          <div>Laporan telah diverifikasi oleh Tim Accounting dengan daftar berikut:</div>
                      </div>
                      <div class="position">
                          <table class="demo-table">
                              <thead>
                                  <tr>
                                      <th>No</th>
                                      <th>Nama Area</th>
                                      <th>Nama File</th>
                                      <th>Jenis Report</th>
                                      <th>Tanggal Report</th>
                                      <th>Tanggal Upload</th>
                                      <th>Tanggal Verifikasi</th>
                                      <th>Status</th>
                                      <th>Alasan</th>
                                  </tr>
                              </thead>
                              <tbody>
                                ${tableTd}
                              </tbody>
                          </table>
                      </div>
                      <div class="martit3">Mohon untuk perbaiki dan kirim ulang dokumen dengan status reject</div>
                      <a href="http://accounting.pinusmerahabadi.co.id/">Klik link berikut untuk akses web accounting</a>
                      <div class="tittle foot">
                          Terima kasih,
                      </div>
                      <div class="tittle foot1">
                          Regards,
                      </div>
                      <div class="tittle">
                          Team Accounting
                      </div>
                  </body>
                  `
                }
                const sendEmail = await wrapMail.wrapedSendMail(mailOptions)
                if (sendEmail) {
                  return response(res, 'success send email')
                } else {
                  return response(res, 'failed to send email', {}, 401, false)
                }
              } else {
                return response(res, 'failed to send email', {}, 401, false)
              }
            } else {
              return response(res, 'kode plant not found', {}, 401, false)
            }
          } else {
            return response(res, 'failed to send email', { }, 401, false)
          }
        } else {
          return response(res, 'failed to send email', { }, 401, false)
        }
      } else {
        return response(res, "You're not super administrator", {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  sendMailArea: async (req, res) => {
    try {
      const level = req.user.level
      // const id = req.params.id
      const list = Object.values(req.body)
      if (level === 4 || level === 5) {
        const dok = await Path.findByPk(list[0])
        if (dok) {
          const act = await activity.findByPk(dok.activityId)
          if (act) {
            const result = await email.findOne({
              where: {
                [Op.and]: [
                  { kode_plant: act.kode_plant },
                  { tipe: act.tipe }
                ]
              }
            })
            if (result) {
              const find = await depo.findOne({
                where: {
                  [Op.or]: [
                    { kode_plant: result.kode_plant },
                    { kode_depo: result.kode_plant }
                  ]
                  // kode_plant: result.kode_plant
                }
              })
              if (find) {
                let tableTd = ''
                const cek = []
                for (let i = 0; i < list.length; i++) {
                  const findDoc = await Path.findByPk(list[i])
                  if (findDoc) {
                    const temp = findDoc.status_dokumen
                    const status = temp === 0 || temp === 6 ? 'Reject' : temp === 3 || temp === 5 ? 'Approve' : temp === null ? 'Belum upload' : 'Sudah upload'
                    const element = `
                        <tr>
                          <td>${i + 1}</td>
                          <td>${find.nama_depo}</td>
                          <td>${findDoc.dokumen}</td>
                          <td>${act.jenis_dokumen}</td>
                          <td>${moment(act.createdAt).subtract(1, 'day').format('DD-MM-YYYY')}</td>
                          <td>${moment(findDoc.createdAt).format('DD-MM-YYYY')}</td>
                          <td>${moment(findDoc.updatedAt).format('DD-MM-YYYY')}</td>
                          <td>${status}</td>
                          <td>${findDoc.alasan === null ? '' : findDoc.alasan}</td>
                        </tr>`
                    tableTd = tableTd + element
                    cek.push(1)
                  }
                }
                const mailOptions = {
                  from: 'noreply_acc@pinusmerahabadi.co.id',
                  replyTo: 'noreply_acc@pinusmerahabadi.co.id',
                  to: `${result.email_ho_pic}`,
                  cc: `${result.email_sa_kasir}, ${result.email_aos}`,
                  // to: 'insfopma@gmail.com',
                  // cc: 'fahmiazis797@gmail.com',
                  subject: 'Info Dokumen Report',
                  html: `<head>
                    <style type="text/css">
                      body {
                          display: flexbox;
                          flex-direction: column;
                      }
                      .tittle {
                          font-size: 15px;
                      }
                      .mar {
                          margin-bottom: 20px;
                      }
                      .mar1 {
                          margin-bottom: 10px;
                      }
                      .foot {
                          margin-top: 20px;
                          margin-bottom: 10px;
                      }
                      .foot1 {
                          margin-bottom: 50px;
                      }
                      .position {
                          display: flexbox;
                          flex-direction: row;
                          justify-content: left;
                          margin-top: 10px;
                      }
                      table {
                          font-family: "Lucida Sans Unicode", "Lucida Grande", "Segoe Ui";
                          font-size: 12px;
                      }
                      .demo-table {
                          border-collapse: collapse;
                          font-size: 13px;
                      }
                      .demo-table th, 
                      .demo-table td {
                          border-bottom: 1px solid #e1edff;
                          border-left: 1px solid #e1edff;
                          padding: 7px 17px;
                      }
                      .demo-table th, 
                      .demo-table td:last-child {
                          border-right: 1px solid #e1edff;
                      }
                      .demo-table td:first-child {
                          border-top: 1px solid #e1edff;
                      }
                      .demo-table td:last-child{
                          border-bottom: 0;
                      }
                      caption {
                          caption-side: top;
                          margin-bottom: 10px;
                      }
                      
                      /* Table Header */
                      .demo-table thead th {
                          background-color: #508abb;
                          color: #FFFFFF;
                          border-color: #6ea1cc !important;
                          text-transform: uppercase;
                      }
                      
                      /* Table Body */
                      .demo-table tbody td {
                          color: #353535;
                      }
                      
                      .demo-table tbody tr:nth-child(odd) td {
                          background-color: #f4fbff;
                      }
                      .demo-table tbody tr:hover th,
                      .demo-table tbody tr:hover td {
                          background-color: #ffffa2;
                          border-color: #ffff0f;
                          transition: all .2s;
                      }
                      .martit2 {
                          font-size: 15px;
                          margin-top: 20px;
                          margin-bottom: 20px;
                          font-weight: bold;
                      }
                      .martit {
                          font-size: 15px;
                          margin-bottom: 20px;
                          font-weight: bold;
                      }
                      .martit3 {
                          font-size: 15px;
                          margin-top: 10px;
                      }
                    </style>
                  </head>
                  <body>
                      <div class="martit">
                          Dear Bapak/Ibu Team accounting,
                      </div>
                      <div class="tittle mar1">
                          <div>Laporan telah diverifikasi oleh Tim Accounting dengan daftar berikut:</div>
                      </div>
                      <div class="position">
                          <table class="demo-table">
                              <thead>
                                  <tr>
                                      <th>No</th>
                                      <th>Nama Area</th>
                                      <th>Nama File</th>
                                      <th>Jenis Report</th>
                                      <th>Tanggal Report</th>
                                      <th>Tanggal Upload</th>
                                      <th>Tanggal Verifikasi</th>
                                      <th>Status</th>
                                      <th>Alasan</th>
                                  </tr>
                              </thead>
                              <tbody>
                                ${tableTd}
                              </tbody>
                          </table>
                      </div>
                      <div class="martit3">Mohon untuk perbaiki dan kirim ulang dokumen dengan status reject</div>
                      <a href="http://accounting.pinusmerahabadi.co.id/">Klik link berikut untuk akses web accounting</a>
                      <div class="tittle foot">
                          Terima kasih,
                      </div>
                      <div class="tittle foot1">
                          Regards,
                      </div>
                      <div class="tittle">
                          Team Accounting
                      </div>
                  </body>
                  `
                }
                const sendEmail = await wrapMail.wrapedSendMail(mailOptions)
                if (sendEmail) {
                  return response(res, 'success send email')
                } else {
                  return response(res, 'failed to send email', {}, 401, false)
                }
              } else {
                return response(res, 'failed to send email', {}, 401, false)
              }
            } else {
              return response(res, 'kode plant not found', {}, 401, false)
            }
          } else {
            return response(res, 'failed to send email', { }, 401, false)
          }
        } else {
          return response(res, 'failed to send email', { }, 401, false)
        }
      } else {
        return response(res, "You're not super administrator", {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  reportDokumenOld: async (req, res) => {
    req.setTimeout(1000 * 60 * 30)
    try {
      const { level, kode: depoKode } = req.user
      const { from, to, tipe } = req.query

      const timeFrom = from
        ? moment(from).startOf('day').format('YYYY-MM-DD HH:mm:ss')
        : moment().startOf('day').format('YYYY-MM-DD HH:mm:ss')
      const timeTo = to
        ? moment(to).endOf('day').format('YYYY-MM-DD HH:mm:ss')
        : moment().endOf('day').format('YYYY-MM-DD HH:mm:ss')
      const tipeValue = tipe || 'daily'

      const schema = joi.object({
        kode_plant: joi.string().allow(''),
        pic: joi.string().allow(''),
        spv: joi.string().allow('')
      })
      const { value: results, error } = schema.validate(req.body)
      if (error) return response(res, 'Error', { error: error.message }, 400, false)

      // build replacements & filter safely
      const replacements = {
        tipeValue,
        timeFrom,
        timeTo,
        jenisPattern: `%${tipeValue}%`
      }

      let filterClause = ''
      if ([1, 2, 3].includes(level)) {
        const conditions = []
        if (results.pic && results.pic !== 'all') {
          conditions.push('p.pic LIKE :picLike')
          replacements.picLike = `%${results.pic}%`
        }
        if (results.spv && results.spv !== '') {
          conditions.push('p.spv LIKE :spvLike')
          replacements.spvLike = `%${results.spv}%`
        }
        if (results.kode_plant && results.kode_plant !== 'all') {
          conditions.push('d.kode_plant = :kodePlant')
          replacements.kodePlant = results.kode_plant
        }
        filterClause = conditions.length ? 'WHERE ' + conditions.join(' OR ') : ''
      } else if ([4, 5].includes(level)) {
        filterClause = 'WHERE d.kode_plant = :depoKode'
        replacements.depoKode = depoKode
      }

      // RAW flat query — note: pics alias = p, Paths alias = path (tidak memakai p.dokumen)
      const mainQuery = `
        SELECT
          d.kode_plant,
          d.nama_depo,
          d.profit_center,
          d.kode_sap_1,
          d.status_depo,
          a.id AS activity_id,
          a.createdAt AS activity_date,
          a.progress AS activity_progress,
          path.dokumen AS dokumen,
          path.status_dokumen AS status_dokumen,
          path.createdAt AS dokumen_created
        FROM depos d
        LEFT JOIN pics p ON p.kode_depo = d.kode_plant
        LEFT JOIN activities a
          ON a.kode_plant = d.kode_plant
          AND a.jenis_dokumen = :tipeValue
          AND a.createdAt BETWEEN :timeFrom AND :timeTo
          AND a.progress > 0
        LEFT JOIN Paths path ON path.activityId = a.id
        ${filterClause}
        ORDER BY d.nama_depo ASC, a.createdAt ASC
      `

      const rows = await sequelize.query(mainQuery, {
        replacements,
        type: sequelize.QueryTypes.SELECT
      })

      if (!rows.length) return response(res, 'Data not found', {}, 404, false)

      // ambil nama dokumen (unik) — pake replacements juga
      const dokumenRows = await sequelize.query(
        'SELECT DISTINCT nama_dokumen FROM documents WHERE jenis_dokumen LIKE :jenisPattern ORDER BY LOWER(nama_dokumen) ASC',
        { replacements, type: sequelize.QueryTypes.SELECT }
      )
      const dokumenNames = dokumenRows.map(r => r.nama_dokumen)

      // jumlah dokumen per depo: join documents -> depos via status_depo (sesuai relasi kamu)
      const dokumenCountRows = await sequelize.query(
        `SELECT d.kode_plant, COUNT(doc.id) AS jumlah_dokumen
         FROM documents doc
         JOIN depos d ON doc.status_depo = d.status_depo
         WHERE doc.jenis_dokumen LIKE :jenisPattern
         GROUP BY d.kode_plant`,
        { replacements, type: sequelize.QueryTypes.SELECT }
      )
      const dokumenCountMap = {}
      dokumenCountRows.forEach(dc => { dokumenCountMap[dc.kode_plant] = dc.jumlah_dokumen })

      // mapping depo -> activities -> docs
      const depoMap = {}
      for (const row of rows) {
        const kode = row.kode_plant || '_unknown_'
        if (!depoMap[kode]) {
          depoMap[kode] = {
            kode_plant: row.kode_plant,
            nama_depo: row.nama_depo,
            profit_center: row.profit_center,
            kode_sap_1: row.kode_sap_1,
            status_depo: row.status_depo,
            dokumen_count: dokumenCountMap[row.kode_plant] || 0,
            activities: {}
          }
        }

        if (row.activity_id) {
          if (!depoMap[kode].activities[row.activity_id]) {
            depoMap[kode].activities[row.activity_id] = {
              createdAt: row.activity_date,
              progress: row.activity_progress,
              docs: []
            }
          }
          if (row.dokumen) {
            depoMap[kode].activities[row.activity_id].docs.push({
              dokumen: row.dokumen,
              status_dokumen: row.status_dokumen,
              createdAt: row.dokumen_created
            })
          }
        }
      }

      // build header & body (skip activities with zero progress already filtered in SQL)
      const header = buildHeader(dokumenNames)
      const body = []
      let no = 1
      for (const depo of Object.values(depoMap)) {
        const activities = Object.values(depo.activities)
        if (activities.length) {
          activities.forEach((act, idx) => {
            let progressCount = 0
            const row = []
            row.push(`${no}.${idx + 1}`)
            row.push(moment(act.createdAt).format('LL'))
            row.push(depo.nama_depo)
            row.push(depo.kode_plant)
            row.push(depo.profit_center)
            row.push(depo.kode_sap_1)
            row.push(depo.status_depo)

            for (const nama of dokumenNames) {
              const doc = act.docs.find(d => d.dokumen === nama)
              if (doc && (doc.status_dokumen === 3 || doc.status_dokumen === 5)) progressCount++
              row.push(doc ? (doc.status_dokumen === 3 ? moment(doc.createdAt).format('LLL') : `Telat (${moment(doc.createdAt).format('LLL')})`) : '-')
            }

            const percent = depo.dokumen_count > 0 ? `${Math.round((progressCount / depo.dokumen_count) * 100)}%` : '0%'
            row.push(depo.dokumen_count, progressCount, percent)
            body.push(row)
          })
        } else {
          const row = []
          row.push(no)
          row.push('-')
          row.push(depo.nama_depo)
          row.push(depo.kode_plant)
          row.push(depo.profit_center)
          row.push(depo.kode_sap_1)
          row.push(depo.status_depo)
          dokumenNames.forEach(() => row.push('-'))
          row.push(depo.dokumen_count, 0, '0%')
          body.push(row)
        }
        no++
      }

      return response(res, 'success', { data: [header, ...body] })
    } catch (err) {
      return response(res, err.message, {}, 500, false)
    }
  },
  reportDokumen: async (req, res) => {
    req.setTimeout(1000 * 60 * 30)
    try {
      const { level, kode: depoKode } = req.user
      const { from, to, tipe } = req.query

      const timeFrom = from
        ? moment(from).startOf('day').format('YYYY-MM-DD HH:mm:ss')
        : moment().startOf('day').format('YYYY-MM-DD HH:mm:ss')
      const timeTo = to
        ? moment(to).endOf('day').format('YYYY-MM-DD HH:mm:ss')
        : moment().endOf('day').format('YYYY-MM-DD HH:mm:ss')
      const tipeValue = tipe || 'daily'

      // Validasi body
      const schema = joi.object({
        kode_plant: joi.string().allow(''),
        pic: joi.string().allow(''),
        spv: joi.string().allow('')
      })
      const { value: results, error } = schema.validate(req.body)
      if (error) return response(res, 'Error', { error: error.message }, 400, false)

      // 1️⃣ Ambil semua depo dulu
      const depoFilter = {}
      if ([1, 2, 3].includes(level)) {
        if (results.kode_plant && results.kode_plant !== 'all') depoFilter.kode_plant = results.kode_plant
      } else if ([4, 5].includes(level)) {
        depoFilter.kode_plant = depoKode
      }
      const depos = await depo.findAll({ where: depoFilter, raw: true })

      if (!depos.length) return response(res, 'Data not found', {}, 404, false)

      // 2️⃣ Ambil semua dokumen unik untuk header
      const dokumenRows = await sequelize.query(
        'SELECT DISTINCT nama_dokumen FROM documents WHERE jenis_dokumen LIKE :tipeValue ORDER BY LOWER(nama_dokumen) ASC',
        { replacements: { tipeValue: `%${tipeValue}%` }, type: sequelize.QueryTypes.SELECT }
      )
      const dokumenNames = dokumenRows.map(r => r.nama_dokumen)

      // 3️⃣ Ambil semua activities + paths dalam range
      const activityRows = await sequelize.query(
        `SELECT a.id AS activity_id, a.kode_plant, a.createdAt AS activity_date, a.progress,
                path.dokumen, path.status_dokumen, path.createdAt AS dokumen_created
         FROM activities a
         LEFT JOIN Paths path ON path.activityId = a.id
         WHERE a.jenis_dokumen = :tipeValue
         AND a.createdAt BETWEEN :timeFrom AND :timeTo
         AND a.progress > 0`,
        { replacements: { tipeValue, timeFrom, timeTo }, type: sequelize.QueryTypes.SELECT }
      )

      // 4️⃣ Ambil jumlah dokumen per depo
      const dokumenCountRows = await sequelize.query(
        `SELECT d.kode_plant, COUNT(doc.id) AS jumlah_dokumen
         FROM documents doc
         JOIN depos d ON doc.status_depo = d.status_depo
         WHERE doc.jenis_dokumen LIKE :tipeValue
         GROUP BY d.kode_plant`,
        { replacements: { tipeValue: `%${tipeValue}%` }, type: sequelize.QueryTypes.SELECT }
      )
      const dokumenCountMap = {}
      dokumenCountRows.forEach(dc => { dokumenCountMap[dc.kode_plant] = dc.jumlah_dokumen })

      // 5️⃣ Mapping depo -> activities -> docs
      const depoMap = {}
      depos.forEach(d => {
        depoMap[d.kode_plant] = { ...d, dokumen_count: dokumenCountMap[d.kode_plant] || 0, activities: {} }
      })
      activityRows.forEach(a => {
        if (!depoMap[a.kode_plant]) return
        if (!depoMap[a.kode_plant].activities[a.activity_id]) {
          depoMap[a.kode_plant].activities[a.activity_id] = {
            createdAt: a.activity_date,
            progress: a.progress,
            docs: []
          }
        }
        if (a.dokumen) {
          depoMap[a.kode_plant].activities[a.activity_id].docs.push({
            dokumen: a.dokumen,
            status_dokumen: a.status_dokumen,
            createdAt: a.dokumen_created
          })
        }
      })

      // 6️⃣ Build header & body
      const header = buildHeader(dokumenNames)
      const body = []
      let no = 1
      Object.values(depoMap).forEach(depo => {
        const activities = Object.values(depo.activities)
        if (activities.length) {
          activities.forEach((act, idx) => {
            let progressCount = 0
            const row = []
            row.push(`${no}.${idx + 1}`)
            row.push(moment(act.createdAt).format('LL'))
            row.push(depo.nama_depo)
            row.push(depo.kode_plant)
            row.push(depo.profit_center)
            row.push(depo.kode_sap_1)
            row.push(depo.status_depo)

            dokumenNames.forEach(nama => {
              const doc = act.docs.find(d => d.dokumen === nama)
              if (doc && (doc.status_dokumen === 3 || doc.status_dokumen === 5)) progressCount++
              row.push(doc ? (doc.status_dokumen === 3 ? moment(doc.createdAt).format('LLL') : `Telat (${moment(doc.createdAt).format('LLL')})`) : '-')
            })

            const percent = depo.dokumen_count > 0 ? `${Math.round((progressCount / depo.dokumen_count) * 100)}%` : '0%'
            row.push(depo.dokumen_count, progressCount, percent)
            body.push(row)
          })
        } else {
          const row = []
          row.push(no)
          row.push('-')
          row.push(depo.nama_depo)
          row.push(depo.kode_plant)
          row.push(depo.profit_center)
          row.push(depo.kode_sap_1)
          row.push(depo.status_depo)
          dokumenNames.forEach(() => row.push('-'))
          row.push(depo.dokumen_count, 0, '0%')
          body.push(row)
        }
        no++
      })

      return response(res, 'success', { data: [header, ...body] })
    } catch (err) {
      return response(res, err.message, {}, 500, false)
    }
  },
  getNotif: async (req, res) => {
    try {
      const kode = req.user.kode
      const level = req.user.level
      const name = req.user.name
      const now = new Date(moment().format('YYYY-MM-DD'))
      const tomo = new Date(moment().format('YYYY-MM-DD 24:00:00'))
      console.log(kode)
      console.log()
      if (level === 4) {
        const result = await notif.findAll({
          where: {
            [Op.and]: [
              { kode_plant: kode },
              { tipe: 'sa' }
            ],
            reject: 'true'
          },
          order: [['id', 'DESC']],
          include: [
            {
              model: Path,
              as: 'dokumen'
            },
            {
              model: activity,
              as: 'active'
            }
          ]
        })
        if (result) {
          return response(res, 'success get notif', { result })
        } else {
          return response(res, 'failed get notif', {}, 404, false)
        }
      } else if (level === 5) {
        const result = await notif.findAll({
          where: {
            [Op.and]: [
              { kode_plant: kode },
              { tipe: 'kasir' }
            ],
            reject: 'true'
          },
          order: [['id', 'DESC']],
          include: [
            {
              model: Path,
              as: 'dokumen'
            },
            {
              model: activity,
              as: 'active'
            }
          ]
        })
        if (result) {
          return response(res, 'success get notif', { result })
        } else {
          return response(res, 'failed get notif', {}, 404, false)
        }
      } else if (level === 2) {
        const find = await pic.findAndCountAll({
          where: {
            spv: name
          },
          include: [
            {
              model: depo,
              as: 'depo'
            }
          ]
        })
        if (find) {
          const depos = []
          find.rows.map(x => {
            return (
              depos.push(x)
            )
          })
          if (depos.length > 0) {
            const sa = []
            const kasir = []
            for (let i = 0; i < depos.length; i++) {
              const result = await notif.findAndCountAll({
                where: {
                  [Op.and]: [
                    { kode_plant: depos[i].kode_depo },
                    { upload: 'true' }
                  ],
                  createdAt: {
                    [Op.lt]: tomo,
                    [Op.gt]: now
                  }
                },
                order: [['id', 'ASC']],
                limit: 1,
                include: [
                  {
                    model: Path,
                    as: 'dokumen'
                  },
                  {
                    model: activity,
                    as: 'active'
                  }
                ]
              })
              if (result.rows[0]) {
                result.rows.map(item => {
                  return sa.push(item)
                })
              }
            }
            if (sa.length > 0) {
              return response(res, 'list dokumen', { sa, kasir })
            } else {
              return response(res, 'list dokumen', { sa, kasir })
            }
          } else {
            return response(res, 'depo no found', {}, 404, false)
          }
        } else {
          return response(res, 'failed to get notif', {}, 404, false)
        }
      } else if (level === 3) {
        const find = await pic.findAndCountAll({
          where: {
            pic: name
          },
          include: [
            {
              model: depo,
              as: 'depo'
            }
          ]
        })
        if (find) {
          const depos = []
          find.rows.map(x => {
            return (
              depos.push(x)
            )
          })
          if (depos.length > 0) {
            const sa = []
            const kasir = []
            for (let i = 0; i < depos.length; i++) {
              const result = await notif.findAndCountAll({
                where: {
                  [Op.and]: [
                    { kode_plant: depos[i].kode_depo },
                    { upload: 'true' }
                  ],
                  createdAt: {
                    [Op.lt]: tomo,
                    [Op.gt]: now
                  }
                },
                order: [['id', 'ASC']],
                limit: 1,
                include: [
                  {
                    model: Path,
                    as: 'dokumen'
                  },
                  {
                    model: activity,
                    as: 'active'
                  }
                ]
              })
              if (result.rows[0]) {
                result.rows.map(item => {
                  return sa.push(item)
                })
              }
            }
            if (sa.length > 0 || kasir.length > 0) {
              return response(res, 'list dokumen', { sa, kasir })
            } else {
              return response(res, 'list dokumen', { sa, kasir })
            }
          } else {
            return response(res, 'depo no found', {}, 404, false)
          }
        } else {
          return response(res, 'failed to get notif', {}, 404, false)
        }
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  // updateNotif: async (req, res) => {
  //   try {
  //     const id = req.params.id
  //     const level = req.user.level
  //     if (level === 2) {
  //       const result = await notif.findByPk(id)

  //     } else {
  //     }
  //   } catch (error) {
  //     return response(res, error.message, {}, 500, false)
  //   }
  // },
  getAllActivity: async (req, res) => {
    try {
      const level = req.user.level
      const name = req.user.name
      let { limit, page, search, time, tipe, find } = req.query
      let searchValue = ''
      let timeValue = ''
      let tipeValue = ''
      let findValue = ''
      if (typeof search === 'object') {
        searchValue = Object.values(search)[0]
      } else {
        searchValue = search || ''
      }
      if (typeof find === 'object') {
        findValue = Object.values(find)[0]
      } else {
        findValue = find || ''
      }
      if (typeof time === 'object') {
        timeValue = Object.values(time)[0]
      } else {
        timeValue = time || '' // eslint-disable-line
      }
      if (typeof tipe === 'object') {
        tipeValue = Object.values(tipe)[0]
      } else {
        tipeValue = tipe || 'daily'
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
      let now = new Date(moment().clone().startOf('month').format('YYYY-MM-DD'))
      let tomo = new Date(moment().add(1, 'month').endOf('month').format('YYYY-MM-DD'))
      if (tipeValue === 'monthly') {
        now = new Date(moment().clone().startOf('year').format('YYYY-MM-DD'))
        tomo = new Date(moment().clone().endOf('year').format('YYYY-MM-DD'))
      }
      if (level === 2) {
        const results = await pic.findAndCountAll({
          where: {
            spv: name
          },
          limit: limit,
          offset: (page - 1) * limit,
          include: [
            {
              model: depo,
              as: 'depo',
              where: {
                [Op.or]: [
                  { kode_plant: { [Op.like]: `%${findValue}%` } },
                  { nama_depo: { [Op.like]: `%${findValue}%` } },
                  { home_town: { [Op.like]: `%${findValue}%` } }
                ]
              }
            }
          ]
        })
        const pageInfo = pagination('/dashboard/active', req.query, page, limit, results.count)
        if (results) {
          const depos = []
          results.rows.map(x => {
            return (
              depos.push(x)
            )
          })
          if (depos.length > 0) {
            const sa = []
            const kasir = []
            for (let i = 0; i < depos.length; i++) {
              const result = await depo.findAndCountAll({
                where: {
                  kode_plant: depos[i].kode_depo
                },
                include: [
                  {
                    model: activity,
                    as: 'active',
                    where: {
                      [Op.and]: [
                        { kode_plant: depos[i].kode_depo },
                        { tipe: 'sa' },
                        { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } }
                      ],
                      createdAt: {
                        [Op.lt]: tomo,
                        [Op.gt]: now
                      }
                    },
                    limit: 31,
                    include: [
                      {
                        model: Path,
                        as: 'doc',
                        limit: 50
                      }
                    ]
                  },
                  {
                    model: documents,
                    as: 'dokumen',
                    where: {
                      [Op.or]: [
                        { nama_dokumen: { [Op.like]: `%${searchValue}%` } }
                      ],
                      [Op.and]: [
                        { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } },
                        { uploadedBy: 'sa' }
                      ],
                      [Op.not]: { status: 'inactive' }
                    }
                  }
                ]
              })
              if (result) {
                sa.push(result.rows[0])
              }
            }
            for (let i = 0; i < depos.length; i++) {
              const result = await depo.findAndCountAll({
                where: {
                  kode_plant: depos[i].kode_depo
                },
                include: [
                  {
                    model: activity,
                    as: 'active',
                    where: {
                      [Op.and]: [
                        { kode_plant: depos[i].kode_depo },
                        { tipe: 'kasir' },
                        { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } }
                      ],
                      createdAt: {
                        [Op.lt]: tomo,
                        [Op.gt]: now
                      }
                    },
                    limit: 31,
                    include: [
                      {
                        model: Path,
                        as: 'doc',
                        limit: 50
                      }
                    ]
                  },
                  {
                    model: documents,
                    as: 'dokumen',
                    where: {
                      [Op.or]: [
                        { nama_dokumen: { [Op.like]: `%${searchValue}%` } }
                      ],
                      [Op.and]: [
                        { jenis_dokumen: { [Op.like]: `%${tipeValue}%` } },
                        { uploadedBy: 'kasir' }
                      ],
                      [Op.not]: { status: 'inactive' }
                    }
                  }
                ]
              })
              if (result) {
                kasir.push(result.rows[0])
              }
            }
            if (sa.length > 0 || kasir.length > 0) {
              return response(res, 'list dokumen', { results, sa, kasir, pageInfo })
            } else {
              return response(res, 'list dokumen', { results, sa, kasir, pageInfo })
            }
          } else {
            return response(res, 'depo no found', {}, 404, false)
          }
        } else {
          return response(res, 'failed to get dokumen', {}, 404, false)
        }
      } else {
        return response(res, "you're not user spv", {}, 404, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  editAccessActive: async (req, res) => {
    try {
      const id = req.params.id
      const level = req.user.level
      const schema = joi.object({
        access: joi.string().valid('lock', 'unlock')
      })
      const { value: results, error } = schema.validate(req.body)
      if (error) {
        return response(res, 'Error', { error: error.message }, 404, false)
      } else {
        if (level === 2) {
          const result = await activity.findByPk(id)
          if (result) {
            await result.update(results)
            return response(res, 'success update activity')
          } else {
            return response(res, 'failed update activity', {}, 404, false)
          }
        } else {
          return response(res, "you're not user spv", {}, 404, false)
        }
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  testGetDocument: async (req, res) => {
    try {
      const kode = req.user.kode
      const result = await depo.findOne({
        where: {
          [Op.or]: [
            { kode_plant: kode },
            { kode_depo: kode }
          ]
          // kode_plant: kode
        }
      })
      if (result) {
        const cabang = result.status_depo
        const findDoc = await documents.findAndCountAll({
          where: {
            [Op.or]: [
              { nama_dokumen: { [Op.like]: '%%' } }
            ],
            [Op.and]: [
              {
                [Op.or]: [
                  { access: { [Op.like]: `%${kode}%` } },
                  { access: null }
                ]
              },
              { status_depo: cabang },
              { uploadedBy: 'kasir' },
              { jenis_dokumen: { [Op.like]: '%daily%' } }
            ],
            [Op.not]: { status: 'inactive' }
          },
          order: [['id', 'DESC']]
        })
        if (findDoc) {
          return response(res, 'success', { findDoc })
        } else {
          return response(res, 'failed', {}, 400, false)
        }
      } else {
        return response(res, 'failed', {}, 400, false)
      }
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  },
  debugDownload: async (req, res) => {
    try {
      const { startDate, endDate, namaFile } = req.query

      if (!startDate || !endDate) {
        return res.status(400).json({
          success: false,
          message: 'startDate dan endDate harus diisi'
        })
      }

      const start = moment(startDate).format('YYYY-MM-DD')
      const end = moment(endDate).format('YYYY-MM-DD')

      let sqlQuery = `
        SELECT 
          id,
          path,
          dokumen,
          kode_depo,
          activityId,
          status_dokumen,
          updatedAt,
          createdAt
        FROM Paths
        WHERE 
          path IS NOT NULL
          AND path != ''
          AND DATE(updatedAt) BETWEEN :start AND :end
      `

      const replacements = { start, end }

      if (namaFile) {
        sqlQuery += ' AND dokumen LIKE :namaFile'
        replacements.namaFile = `%${namaFile}%`
      }

      sqlQuery += ' ORDER BY updatedAt DESC LIMIT 10'

      const files = await sequelize.query(sqlQuery, {
        replacements,
        type: QueryTypes.SELECT
      })

      // Check file existence and generate new names
      const fileCheck = files.slice(0, 5).map(file => {
        const filePath = path.join(__dirname, '../../', file.path)
        const originalFileName = path.basename(file.path)
        const sanitizedDokumen = file.dokumen.replace(/[<>:"/\\|?*]/g, '-')
        const newFileName = `${file.kode_depo}-${sanitizedDokumen}-${originalFileName}`

        return {
          ...file,
          fileExists: fs.existsSync(filePath),
          fullPath: filePath,
          originalFileName: originalFileName,
          newFileName: newFileName,
          sanitizedDokumen: sanitizedDokumen
        }
      })

      return res.json({
        success: true,
        debug: {
          query: {
            startDate,
            endDate,
            namaFile,
            start_formatted: start,
            end_formatted: end
          },
          sql: sqlQuery,
          replacements: replacements,
          results: {
            count: files.length,
            sample: fileCheck
          }
        }
      })
    } catch (error) {
      return res.status(500).json({
        success: false,
        message: error.message,
        stack: error.stack
      })
    }
  },

  downloadDocuments: async (req, res) => {
    try {
      const { startDate, endDate, namaFile } = req.query

      // Validasi input
      if (!startDate || !endDate) {
        return res.status(400).json({
          success: false,
          message: 'startDate dan endDate harus diisi'
        })
      }

      // Setup date range
      const start = moment(startDate).format('YYYY-MM-DD')
      const end = moment(endDate).format('YYYY-MM-DD')

      // Query langsung ke Paths
      let sqlQuery = `
        SELECT 
          id,
          path,
          dokumen,
          kode_depo,
          updatedAt
        FROM Paths
        WHERE 
          path IS NOT NULL
          AND path != ''
          AND DATE(updatedAt) BETWEEN :start AND :end
      `

      const replacements = { start, end }

      if (namaFile) {
        sqlQuery += ' AND dokumen LIKE :namaFile'
        replacements.namaFile = `%${namaFile}%`
      }

      sqlQuery += ' ORDER BY updatedAt DESC'

      // Execute raw query
      const files = await sequelize.query(sqlQuery, {
        replacements,
        type: QueryTypes.SELECT
      })

      if (files.length === 0) {
        return res.status(404).json({
          success: false,
          message: 'Tidak ada file yang ditemukan pada periode tersebut'
        })
      }

      // Collect and validate file paths
      const filesToZip = []
      const fileStats = {
        found: 0,
        notFound: 0,
        total: files.length
      }

      for (const file of files) {
        const filePath = path.join(__dirname, '../../', file.path)

        // Check if file exists
        if (fs.existsSync(filePath)) {
          // Get original filename
          const originalFileName = path.basename(file.path)

          // Sanitize dokumen name (remove invalid chars for filename)
          const sanitizedDokumen = file.dokumen.replace(/[<>:"/\\|?*]/g, '-')

          // Create new filename: kode_depo-dokumen-originalname
          const newFileName = `${file.kode_depo};${sanitizedDokumen};${moment(file.updatedAt).format('DDMMYYYY')};${originalFileName}`

          filesToZip.push({
            path: filePath,
            originalName: file.path,
            newName: newFileName,
            date: moment(file.updatedAt).format('YYYY-MM-DD'),
            dokumen: sanitizedDokumen,
            kodeDepo: file.kode_depo
          })
          fileStats.found++
        } else {
          console.warn(`File not found: ${filePath}`)
          fileStats.notFound++
        }
      }

      if (filesToZip.length === 0) {
        return res.status(404).json({
          success: false,
          message: `Ditemukan ${files.length} record di database, tetapi tidak ada satupun file yang ada di storage. Kemungkinan file sudah dihapus atau path tidak valid.`,
          stats: {
            totalInDb: files.length,
            foundInStorage: 0
          }
        })
      }

      // Log warning if some files missing
      if (fileStats.notFound > 0) {
        console.warn(`⚠️ Warning: ${fileStats.notFound} dari ${files.length} file tidak ditemukan di storage`)
      }

      // Create zip filename
      const zipFileName = `documents_${moment(startDate).format('YYYYMMDD')}_${moment(endDate).format('YYYYMMDD')}_${Date.now()}.zip`

      // Set headers for download
      res.setHeader('Content-Type', 'application/zip')
      res.setHeader('Content-Disposition', `attachment; filename="${zipFileName}"`)
      res.setHeader('Content-Description', 'File Transfer')
      res.setHeader('Cache-Control', 'no-cache')

      // Create archive and pipe directly to response
      const archive = archiver('zip', {
        zlib: { level: 6 } // Balance between speed and compression
      })

      // Handle archive errors
      archive.on('error', (err) => {
        console.error('Archive error:', err)
        if (!res.headersSent) {
          return res.status(500).json({
            success: false,
            message: 'Error saat membuat zip file',
            error: err.message
          })
        }
      })

      // Log progress
      let processedFiles = 0
      archive.on('progress', (progress) => {
        if (progress.entries.processed % 10 === 0 || progress.entries.processed === progress.entries.total) {
          console.log(`Zipping: ${progress.entries.processed}/${progress.entries.total} files`)
        }
      })

      // Pipe archive to response
      archive.pipe(res)

      // Add files to archive with renamed filename
      filesToZip.forEach(file => {
        try {
          const folderStructure = `${file.date}/${file.dokumen}`

          // PENTING: file.newName adalah nama baru yang akan muncul di dalam zip
          archive.file(file.path, {
            name: `${folderStructure}/${file.newName}`
          })

          console.log(`Added: ${file.newName}`)
          processedFiles++
        } catch (err) {
          console.error(`Error adding file to archive: ${file.path}`, err)
        }
      })

      // Finalize the archive (this triggers the streaming)
      await archive.finalize()

      console.log('✓ ZIP streamed successfully')
      console.log(`  - Total in DB: ${fileStats.total}`)
      console.log(`  - Found: ${fileStats.found}`)
      console.log(`  - Not Found: ${fileStats.notFound}`)
      console.log(`  - Zipped: ${processedFiles}`)
      console.log(`  - Size: ${archive.pointer()} bytes`)
    } catch (error) {
      console.error('Download error:', error)

      if (!res.headersSent) {
        return res.status(500).json({
          success: false,
          message: 'Terjadi kesalahan saat memproses download',
          error: error.message
        })
      }
    }
  },
  getStatistics: async (req, res) => {
    try {
      const kode = req.user.kode
      const level = req.user.level

      // Query untuk mendapatkan data statistik per bulan
      const statistics = await Path.findAll({
        where: {
          [Op.and]: [
            level === 5 || level === 4 ? { kode_depo: kode } : { [Op.not]: { id: null } },
            {
              createdAt: {
                [Op.gte]: moment().subtract(6, 'months').startOf('month').toDate()
              }
            },
            {
              path: {
                [Op.ne]: null
              }
            }
          ]
        },
        attributes: [
          [sequelize.fn('DATE_FORMAT', sequelize.col('createdAt'), '%Y-%m'), 'month'],
          [sequelize.fn('COUNT', sequelize.col('id')), 'total_upload'],
          // Pending: status 1 dan 2
          [sequelize.fn('SUM', sequelize.literal('CASE WHEN status_dokumen IN (1, 2) THEN 1 ELSE 0 END')), 'pending'],
          // Approved: status 3 dan 5
          [sequelize.fn('SUM', sequelize.literal('CASE WHEN status_dokumen IN (3, 5) THEN 1 ELSE 0 END')), 'approved'],
          // Rejected: status 0 dan 6
          [sequelize.fn('SUM', sequelize.literal('CASE WHEN status_dokumen IN (0, 6) THEN 1 ELSE 0 END')), 'rejected'],
          // Late Upload: status 4
          [sequelize.fn('SUM', sequelize.literal('CASE WHEN status_dokumen = 4 THEN 1 ELSE 0 END')), 'late'],
          // Revisi: status 7
          [sequelize.fn('SUM', sequelize.literal('CASE WHEN status_dokumen = 7 THEN 1 ELSE 0 END')), 'revisi']
        ],
        group: ['month'],
        order: [[sequelize.literal('month'), 'ASC']],
        raw: true
      })

      return response(res, 'success', { result: statistics })
    } catch (error) {
      return response(res, error.message, {}, 500, false)
    }
  }
}
