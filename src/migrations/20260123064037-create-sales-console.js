'use strict'
module.exports = {
  up: async (queryInterface, Sequelize) => {
    await queryInterface.createTable('sales_consoles', {
      id: {
        allowNull: false,
        autoIncrement: true,
        primaryKey: true,
        type: Sequelize.INTEGER
      },
      name: {
        type: Sequelize.STRING
      },
      real_name: {
        type: Sequelize.STRING
      },
      note: {
        type: Sequelize.STRING
      },
      path: {
        type: Sequelize.STRING
      },
      type: {
        type: Sequelize.STRING
      },
      user_upload: {
        type: Sequelize.STRING
      },
      date_report: {
        type: Sequelize.DATE
      },
      status: {
        type: Sequelize.INTEGER
      },
      createdAt: {
        allowNull: false,
        type: Sequelize.DATE
      },
      updatedAt: {
        allowNull: false,
        type: Sequelize.DATE
      }
    })
  },
  down: async (queryInterface, Sequelize) => {
    await queryInterface.dropTable('sales_consoles')
  }
}
