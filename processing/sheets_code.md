// Date Validation : Only allow dates that fall within a week
var cells = SpreadsheetApp.getActive().getRange('A2:A');
var rule = SpreadsheetApp.newDataValidation()
    .requireNumberBetween(1, 100)
    .setAllowInvalid(false)
    .setHelpText('Number must be between 1 and 100.')
    .build();
cells.setDataValidation(rule);

// Open on Sheet of Current Week

// Button to add new weekly sheet