define(function (require) {
  var module = require('ui/modules').get('kibana');

  module.constant('quickRanges', [
    { from: 'now/d',    to: 'now/d',    display: 'Today',                 section: 0 },
    { from: 'now/w',    to: 'now/w',    display: 'This week',             section: 0 },
    { from: 'now/M',    to: 'now/M',    display: 'This month',            section: 0 },
    { from: 'now/y',    to: 'now/y',    display: 'This year',             section: 0 },
    { from: 'now/d',    to: 'now',      display: 'The day so far',        section: 0 },
    { from: 'now/w',    to: 'now',      display: 'Week to date',          section: 0 },
    { from: 'now/M',    to: 'now',      display: 'Month to date',         section: 0 },
    { from: 'now/y',    to: 'now',      display: 'Year to date',          section: 0 },

    { from: 'now-1d/d', to: 'now-1d/d', display: 'Yesterday',             section: 1 },
    { from: 'now-2d/d', to: 'now-2d/d', display: 'Day before yesterday',  section: 1 },
    { from: 'now-7d/d', to: 'now-7d/d', display: 'This day last week',    section: 1 },
    { from: 'now-1w/w', to: 'now-1w/w', display: 'Previous week',         section: 1 },
    { from: 'now-1M/M', to: 'now-1M/M', display: 'Previous month',        section: 1 },
    { from: 'now-1y/y', to: 'now-1y/y', display: 'Previous year',         section: 1 },

    { from: 'now-15m',  to: 'now',      display: 'Last 15 minutes',       section: 2 },
    { from: 'now-30m',  to: 'now',      display: 'Last 30 minutes',       section: 2 },
    { from: 'now-1h',   to: 'now',      display: 'Last 1 hour',           section: 2 },
    { from: 'now-4h',   to: 'now',      display: 'Last 4 hours',          section: 2 },
    { from: 'now-12h',  to: 'now',      display: 'Last 12 hours',         section: 2 },
    { from: 'now-24h',  to: 'now',      display: 'Last 24 hours',         section: 2 },
    { from: 'now-7d',   to: 'now',      display: 'Last 7 days',           section: 2 },

    { from: 'now-30d',  to: 'now',      display: 'Last 30 days',          section: 3 },
    { from: 'now-60d',  to: 'now',      display: 'Last 60 days',          section: 3 },
    { from: 'now-90d',  to: 'now',      display: 'Last 90 days',          section: 3 },
    { from: 'now-6M',   to: 'now',      display: 'Last 6 months',         section: 3 },
    { from: 'now-1y',   to: 'now',      display: 'Last 1 year',           section: 3 },
    { from: 'now-2y',   to: 'now',      display: 'Last 2 years',          section: 3 },
    { from: 'now-5y',   to: 'now',      display: 'Last 5 years',          section: 3 },
    
    { from: '2011-01-01',   to: '2011-12-31', display: '2011',            section: 4 },
    { from: '2012-01-01',   to: '2012-12-31', display: '2012',            section: 4 },
    { from: '2013-01-01',   to: '2013-12-31', display: '2013',            section: 4 },
    { from: '2014-01-01',   to: '2014-12-31', display: '2014',            section: 4 },
    { from: '2015-01-01',   to: '2015-12-31', display: '2015',            section: 4 },
    { from: '2016-01-01',   to: '2016-12-31', display: '2016',            section: 4 },

  ]);

});
