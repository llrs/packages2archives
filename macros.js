remark.macros.scale = function(percentage, alt) {
  var url = this;
  return '<img src="' + url + '" style="width: ' + percentage + 'height:'+ percentage + '" alt=' + alt + ' />';
};

remark.macros.height = function(percentage, alt) {
  var url = this;
  return '<img src="' + url + '" style="height: ' + percentage + '" alt=' + alt + ' />';
};

remark.macros.width = function(percentage, alt) {
  var url = this;
  return '<img src="' + url + '" style="width: ' + percentage + '" alt=' + alt + ' />';
};
