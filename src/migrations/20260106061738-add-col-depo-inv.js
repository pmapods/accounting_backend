'use strict'

module.exports = {
  up: async (queryInterface, Sequelize) => {
    await queryInterface.addColumn('inventories', 'keterangan', {
      type: Sequelize.DataTypes.STRING(20)
    })
    await queryInterface.addColumn('inventories', 'singkatan', {
      type: Sequelize.DataTypes.STRING(20)
    })
    await queryInterface.addColumn('inventories', 's_pulau', {
      type: Sequelize.DataTypes.STRING(20)
    })
  },

  down: async (queryInterface, Sequelize) => {
    await queryInterface.removeColumn('inventories', 'keterangan', {})
    await queryInterface.removeColumn('inventories', 'singkatan', {})
    await queryInterface.removeColumn('inventories', 's_pulau', {})
  }
}
