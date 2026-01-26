'use strict'
const {
  Model
} = require('sequelize')
module.exports = (sequelize, DataTypes) => {
  class sales_console extends Model {
    /**
     * Helper method for defining associations.
     * This method is not a part of Sequelize lifecycle.
     * The `models/index` file will call this method automatically.
     */
    static associate (models) {
      // define association here
    }
  };
  sales_console.init({
    name: DataTypes.STRING,
    real_name: DataTypes.STRING,
    note: DataTypes.STRING,
    path: DataTypes.STRING,
    type: DataTypes.STRING,
    date_report: DataTypes.DATE,
    user_upload: DataTypes.STRING,
    status: DataTypes.INTEGER
  }, {
    sequelize,
    modelName: 'sales_console'
  })
  return sales_console
}
