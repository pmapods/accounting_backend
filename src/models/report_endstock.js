'use strict'
const {
  Model
} = require('sequelize')
module.exports = (sequelize, DataTypes) => {
  class report_endstock extends Model {
    /**
     * Helper method for defining associations.
     * This method is not a part of Sequelize lifecycle.
     * The `models/index` file will call this method automatically.
     */
    static associate (models) {
      // define association here
    }
  };
  report_endstock.init({
    name: DataTypes.STRING,
    path: DataTypes.STRING,
    type: DataTypes.STRING,
    status: DataTypes.INTEGER,
    plant: DataTypes.STRING,
    date_report: DataTypes.DATE,
    user_upload: DataTypes.STRING,
    status_report: DataTypes.STRING,
    info: DataTypes.TEXT
  }, {
    sequelize,
    modelName: 'report_endstock'
  })
  return report_endstock
}
