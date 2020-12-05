// Predefined list with colors of task statuses
window._task_status_colors = {
    registered: '#eec4ee',
    defined: '#d2a3d8',
    topreprocess: '#df65b0',
    preprocessing: '#dd1c77',
    tobroken: '#980043',

    assigning: '#dcdc9c',
    ready: '#a2c490',
    pending: '#7fcdbb',
    scouting: '#41b6c4',
    scouted: '#1d91c0',
    running: '#225ea8',
    prepared: '#0c2c84',

    toabort: '#cb8f79',
    aborting: '#fdbe85',
    finishing: '#fd8d3c',
    passed: '#d94701',

    toretry: '#dfdfac',
    toincexec: '#c2e699',
    rerefine: '#78c679',

    done: '#31a354',
    finished: '#006837',

    throttled: '#e2bcad',
    exhausted: '#eea083',
    aborted: '#fb6a4a',
    broken: '#de2d26',
    failed: '#a50f15',

    paused: '#969696'
};
window._task_status_order = ['registered', 'defined', 'topreprocess', 'preprocessing', 'tobroken', 'assigning', 'ready',
    'pending', 'scouting', 'scouted', 'running', 'prepared', 'toabort', 'aborting', 'aborted', 'finishing', 'passed',
    'toretry', 'toincexec', 'rerefine', 'throttled', 'exhausted', 'broken', 'failed', 'done', 'finished', 'paused'];

// Predefined list with colors of job statuses
window._job_status_colors = {
    defined: '#9fb1ee',
    pending: '#9cbfd9',
    waiting: '#6baed6',
    assigned: '#2171b5',

    activated: '#e2c18c',
    sent: '#fdae6b',
    starting: '#e6550d',

    running: '#d2a3d8',
    holding: '#df84c1',
    merging: '#e778a3',
    transferring: '#980043',

    finished: '#31a354',

    cancelled: '#dabdb0',
    unassigned: '#fc9272',
    failed: '#de2d26'
};
window._job_status_order = ['defined', 'pending', 'waiting', 'assigned', 'activated', 'sent', 'starting', 'running',
    'holding', 'transferring', 'merging', 'finished', 'cancelled', 'unassigned', 'failed'];


// List of statuses to be enabled on scatterplots
window._profiling_statuses_enabled = ['defined', 'finished', 'merging', 'failed', 'running', 'activated'];

// Function to remove a specific item from an array
Array.prototype.remove = function() {
    var what, a = arguments, L = a.length, ax;
    while (L && this.length) {
        what = a[--L];
        while ((ax = this.indexOf(what)) !== -1) {
            this.splice(ax, 1);
        }
    }
    return this;
};

// Function to count objects in array
if (!Array.prototype.hasOwnProperty('count'))
    Object.defineProperties(Array.prototype, {
        count: {
            value: function (value) {
                return this.filter(x => x == value).length;
            }
        }
    });

// Function to convert time string to string in HHMMSS format
String.prototype.toHHMMSS = function () {
    var sec_num = parseInt(this, 10); // don't forget the second param
    var hours   = Math.floor(sec_num / 3600);
    var minutes = Math.floor((sec_num - (hours * 3600)) / 60);
    var seconds = sec_num - (hours * 3600) - (minutes * 60);

    if (hours   < 10) {hours   = "0"+hours;}
    if (minutes < 10) {minutes = "0"+minutes;}
    if (seconds < 10) {seconds = "0"+seconds;}
    return hours+':'+minutes+':'+seconds;
};

$(document).ready(()=>TooltipPrepare());

// Function to switch between 'Overall Statistics' and 'Task Execution Analysis' tabs
function ChangeTab(tab){
    $(tab).parent().children().removeClass('header-button-selected');
    $(tab).addClass('header-button-selected');

    if (tab.id === 'header-taskanalysis'){
        $('#parcoords-contents').css({"left": "100vw","opacity": "0"});
        $('#duration-contents').css( {"left": "0", "opacity": "1"});
    }
    else
    {
        $('#header-parallelcoordinates').removeClass('hidden');
        $('#parcoords-contents').css({"left": "0", "opacity": "1"});
        $('#duration-contents').css( {"left": "-100vw", "opacity": "0"});
    }
}

// Send a request to server when the user has chosen the dates on the Overall Statistics tab
function DatesChosen(){
    $('#duration-loading-label').show();
    $('#duration-starting-label').hide();

    $('#dates_loading').show();
    $('#dates_button').hide();

    $('#duration-boxplot').hide();
    $('#duration-boxplot-loading')
        .show()
        .addClass('pc-loading')
        .text('Loading box plot data...');

    let start_time = $('#duration-start').val(),
        end_time = $('#duration-finish').val();

    // Request slowest tasks list
    if (this._ajax_slowest !== undefined) this._ajax_slowest.abort();
    this._ajax_slowest = $.ajax({
        url: '/ajax/request_db',
        data: { 'type': 'get-slowest-tasks', 'start-time': start_time, 'end-time' : end_time},
        dataType: 'json',
        timeout: 0,
        success: (data) => GotDurationData(data),
        error: (error) => RequestError('There was an error: ' + error.statusText)
    });

    // Request boxplot info
    if (this._ajax_boxplot !== undefined) this._ajax_boxplot.abort();
    this._ajax_boxplot = $.ajax({
        url: '/ajax/request_db',
        data: { 'type': 'get-boxplot-information', 'start-time': start_time, 'end-time' : end_time},
        dataType: 'json',
        timeout: 0,
        success: (data) => DrawBoxPlot(data),
        error: (error) => BoxPlotError(error.statusText)
    });
}

// Process the result from DatesChosen() ajax request
function GotDurationData(data){
    if (data.hasOwnProperty('error')){
        RequestError(data.error);
        return;
    }

    $('#duration-description').css('opacity', '0').delay(1000).hide();
    $('.duration-block-right').css('opacity', '1');
    $('#dates_loading').hide();
    $('#dates_button').show();

    // Icons in the table, representing load from server button, loading, ready and error icons and
    // a button to build ParCoords diagram
    let link_btn = (id) => {return '<a href="/task/'+ id +'/" target="_blank" rel="noopener noreferrer" ' +
            'class="duration-img"> ' + numberWithSpaces(parseFloat(Number(id).toFixed(2))) +
            '  <img class="top-link-img info-tooltip" data-type="link" src="/static/images/external-link.png" ' +
            'data-popup-text="Open in new tab"> ' +
            //'class="duration-img duration-load" style="width:10px;height:10px;">' +
            '</a>'; },
        load_btn = '<img data-type="load" data-popup-text="Prepare diagrams" src="/static/images/view.png" ' +
            'class="duration-img duration-load info-tooltip">',
        loading_icon = '<img src="/static/ParallelCoordinates/loading.gif" data-type="loading" ' +
            'data-popup-text="Data is loading" class="duration-img hidden img-unclickable info-tooltip">',
        ready_icon = '<img src="/static/images/checkmark.png" data-type="ready" ' +
            'data-popup-text="Data is ready" class="duration-img hidden info-tooltip">',
        error_icon = '<img src="/static/images/delete.png" data-type="ready" ' +
            'data-popup-text="There was an error. Please try to load the data again." ' +
            'class="duration-img hidden info-tooltip">',
        forward_btn = '<img src="/static/images/line-chart.png" data-type="run" ' +
            'data-popup-text="Draw diagrams" class="duration-img hidden info-tooltip">';

    // Prepare header and cells for the table with the slowest tasks
    this.duration = {
        _storage: {},
        _header: ['TaskID', "Execution time, days", 'Status', 'Diagram controls'].map((x, i) => {
            return {
                title: x,
                className: (i === 0) ? 'firstCol' : '',

                // Add spaces and remove too much numbers after the comma
                "render": function (data, type, full, cell) {
                    if (type === 'display' && !isNaN(data))
                        if (cell.col === 0) return link_btn(data);
                        else return numberWithSpaces(parseFloat(Number(data).toFixed(2)));

                    if (data==='btns') return load_btn + loading_icon + ready_icon + error_icon + forward_btn;

                    return data;
                }
            }
        }),
        _cells: data.data.map(x => x.concat(['btns']))
    };

    // Correct the label
    $('#slowest-tasks-label').html('(Tasks with prodsourcelabel=user from ' +
        document.getElementById('duration-start').value + ' to ' +
        document.getElementById('duration-finish').value + ')');

    // If the table already exists, clear it and fill with new data
    if(this._duration_table !== undefined){
        this._duration_table.clear().draw();
        this._duration_table.rows.add(this.duration._cells);
        this._duration_table.columns.adjust().draw();
    }
    else
        // If not, create a new one
        this._duration_table = $('#slowest-tasks-list')
            .addClass("hover")
            .DataTable({
                data: this.duration._cells,
                columns: this.duration._header,
                mark: true,
                bAutoWidth: false,
                dom: 'ABlfrtip',
                buttons: ['copy', 'csv'],
                "order": [[ 1, "desc" ]],
                "searching": false
            });

    // Listener to prepare tooltips on page change
    this._duration_table.on('draw.dt', () => TooltipPrepare());

    // Add button handlers
    $('#slowest-tasks-list tbody').on( 'click', 'img', function () {
        let jeditaskid = window._duration_table.row($(this).parents('tr')).data()[0].toString();

        if (this.dataset.type === 'run') RunParCoords(jeditaskid);
        else if (['loading', 'link'].includes(this.dataset.type)) return;
        else PreLoadIDinfo(jeditaskid);
    });

    TooltipPrepare();


    if (this._ajax_status_lag !== undefined) this._ajax_status_lag.abort();
    this._ajax_status_lag = $.ajax({
        url: '/ajax/request_db',
        data: { 'type': 'get_tasks_lag', 'ids': data.data.map(x => x[0]).join(',')},
        dataType: 'json',
        timeout: 0,
        success: (data) => GotTasksLagData(data),
        error: (error) => RequestError('There was an error: ' + error.statusText)
    });
}

function GotTasksLagData(data){
    // cols: jeditaskid, modificationtime, status, lag, delay
    var taskid = 0,
        modtimeid = 1,
        statusid = 2,
        lagid = 3,
        delayid = 4;

    let scatterplot_data = function (rows, cols) {
            let unique_statuses = rows.map(x => x[statusid]).filter((x,i,arr) => arr.indexOf(x) === i),
                unique_ids = rows.map(x => x[taskid]).filter((x,i,arr) => arr.indexOf(x) === i),
                // arr will be: [id, date, color, color_val]
                data = [];

            rows.map(val => data.push([val[taskid].toString()]));
            rows.map((val, i) => data[i].push(val[modtimeid]));
            rows.map((val, i) => data[i].push(val[statusid],
                (Object.keys(window._task_status_colors).includes(val[statusid])) ?
                    window._task_status_colors[val[statusid]] :
                    ''));
            rows.map((val, i) => data[i].push((val[delayid] === 'nan') ? '0' : val[delayid].toHHMMSS()));

            return {data: data, unique: {statuses: unique_statuses, ids: unique_ids}};
        },
        scatterplot_config = function(data){
            let //n = 0,
                max_quantity = 20;

            return {
                traces: window._task_status_order.map(x => {
                    let filtered = data.data.filter(y => ((y[statusid] === x))),
                        filtered_rows = [[]];

                    if (filtered.length !== 0)
                        filtered_rows = filtered[0].map((col, i) => filtered.map(row => row[i]));

                    max_quantity = Math.max(filtered_rows[0].length, max_quantity);

                    return {
                        name: x,
                        type: 'scatter',
                        mode: 'markers',
                        //visible: window._profiling_statuses_enabled.includes(x) ? '' : 'legendonly',
                        marker: {
                            color: filtered_rows[3],
                            size: 10 //+ n++
                        },
                        hovertemplate:
                            'Jedi Task ID: %{y:,}<br>' +
                            'Time: %{x}<br>' +
                            'Status: ' + x +
                            ((x==='defined')?'':'<br>Delay: %{text}') ,
                        hoverlabel: { namelength: 0 },
                        y: filtered_rows[taskid],
                        x: filtered_rows[modtimeid],
                        text: filtered_rows[4]
                    }
                }),
                layout: {
                    title: 'Top-50 Slowest Tasks',
                    xaxis:{
                        tickformat: '%H:%M:%S\n%e %b %Y',
                        title: {
                            text: 'Modification Time'
                        },
                        domain: [0.1, 1]
                    },
                    yaxis: {
                        tickformat: 'd',
                        type: 'category',
                        title: {
                            text: 'Jedi Task ID'
                        }
                    },
                    hovermode:'closest',
                    width: 800,
                    height: Math.max((max_quantity < 50) ? max_quantity * 20 : 1000, 500)
                }
            }
        },
        sc_config = scatterplot_config(scatterplot_data(data.data, data.columns));

    Plotly.newPlot('duration-taskstatus_lag', sc_config.traces, sc_config.layout);


    //duration-taskstatus_lag
}

// Function to set the start and finish date on the Overall Statistics page.
// types are: 'last-month' and 'last-day'
function SetDates(type){
    let date = new Date();
    date.setDate(date.getDate() - 1);
    document.getElementById('duration-finish').value = date.toISOString().split('T')[0];

    if (type === 'last-month') date.setMonth(date.getMonth() - 1);
    else date.setDate(date.getDate() - 7);

    document.getElementById('duration-start').value = date.toISOString().split('T')[0];
}

// Draw a box plot from DatesChosen() ajax request
function DrawBoxPlot(data){
    if (data.hasOwnProperty('error')){
        BoxPlotError(data.error);
        return;
    }

    $('#duration-boxplot-loading').hide();
    $('#duration-boxplot').show();

    let plot_data = {
            'done': {y:[], text:[]},
            'finished': {y:[], text:[]},
            'aborted': {y:[], text:[]},
            'failed': {y:[], text:[]},
            'broken': {y:[], text:[]}
        },
        traces = [],
        layout = {
            yaxis: { title: 'Execution time, days' },
            width: 600,
            title: 'Box Plot: Distribution of tasks execution time by statuses'
        },
        config = {};//{responsive: true};

    data.data.forEach((x) => { plot_data[x[2]].text.push(x[0]); plot_data[x[2]].y.push(x[1]); });

    ['done', 'finished', 'aborted', 'failed', 'broken']
        .forEach((x) =>
            traces.push({
                y: plot_data[x].y,
                text: plot_data[x].text,
                type: 'box',
                name: x,
                boxpoints: 'Outliers',
                marker: { color: (window._task_status_colors.hasOwnProperty(x)) ? window._task_status_colors[x] : '' },
                hovertemplate: 'Jedi Task ID: %{text:,}<br>Status: ' + x + '<br>Duration: %{y:.2f} days',
                hoverlabel: { namelength: 0 }
            }));

    Plotly.newPlot('duration-boxplot', traces, layout, config);
}

// If DatesChosen() ajax request fails, this function is called
function BoxPlotError(error){
    $('#duration-boxplot-loading')
        .show()
        .removeClass('pc-loading')
        .text('Error occured. ' + error);
}

// Arrow button in the 'Slowest tasks' table refers to this function
function RunParCoords(id){
    if (!this.duration._storage.hasOwnProperty(id)) PreLoadIDinfo(data[0]);
    else {
        $('#header-parallelcoordinates').click();

        if (!this.hasOwnProperty('parcoords') || this.parcoords._current_id !== id)
            BuildDiagrams(this.duration._storage[id]);
    }
}

// Function to request information about a task from the server
function PreLoadIDinfo(id){
    IDLoading(id);

    $.ajax({
        url: '/ajax/request_db',
        data: { 'type': 'get-id-info', 'jeditaskid': id },
        dataType: 'json',
        timeout: 0,
        success: (data) => StoreData(data),
        error: (error) => RequestError('There was an error: ' +
            (error.status === 0) ? 'Connection to the server failed. Check connection and try again.' :
                error.statusText + '  Click to retry.', id)
    });
}

// Function to store the information about a task into the memory
function StoreData(data){
    if (data.hasOwnProperty('error')){
        RequestError(data.error, (data.hasOwnProperty('jeditaskid')) ? data.jeditaskid : null);
        return;
    }

    this.duration._storage[data.jeditaskid] = data;
    IDLoaded(data.jeditaskid);
}

// Translates taskid in the 'Slowest tasks' table into a table row object
function IDtoTable(id){
    return this.duration._cells.findIndex(x => x[0] === id.toString());
}

// Change buttons in the table when a task info is loading
function IDLoading(id){
    let node = this._duration_table.cell(IDtoTable(id), ':last-child').node();
    $($(node).children()[0]).hide();
    $($(node).children()[1]).show();
    $($(node).children()[2]).hide();
    $($(node).children()[3]).hide();
    $($(node).children()[4]).hide();
}

// Change buttons in the table when a task info is loaded
function IDLoaded(id){
    let node = this._duration_table.cell(IDtoTable(id), ':last-child').node();
    $($(node).children()[0]).hide();
    $($(node).children()[1]).hide();
    $($(node).children()[2]).show();
    $($(node).children()[3]).hide();
    $($(node).children()[4]).show();
}

// Change buttons in the table when a task info request has failed
function IDErrored(id, error = 'There was an error. Please click to load the data again.'){
    let node = this._duration_table.cell(IDtoTable(id), ':last-child').node();
    $($(node).children()[0]).show();
    $($(node).children()[1]).hide();
    $($(node).children()[2]).hide();
    $($(node).children()[3]).show().data('popupText', error + ((error.endsWith('.'))?'':'.') +
        ' Please click to load data again.');
    $($(node).children()[4]).hide();
}

// Send a request to the database about a taskid. Used in /task/ page.
function GetIDinfo(id){
    const value = parseInt(id);

    if (isNaN(value)) {
        alert('Please enter an integer value of the task ID');
        return;
    }

    $('#taskid-selector').hide();
    $('#parcoords-request-loading').show();
    document.title = value + ' -  Parallel Coordinates';

    $.ajax({
        url: '/ajax/request_db',
        data: { 'type': 'get-id-info', 'jeditaskid': value },
        dataType: 'json',
        timeout: 0,
        success: (data) => BuildDiagrams(data),
        error: (error) => RequestError('There was an error: ' + error.statusText, value)
      });

    if (this.hasOwnProperty('duration')) IDLoading(value);
}

// A function to initialize diagrams
function BuildDiagrams(data) {
    // Check for errors
    if (data.hasOwnProperty('error')){
        RequestError(data.error, (data.hasOwnProperty('jeditaskid')) ? data.jeditaskid : null);
        return;
    }

    // Show header and div with diagram
    $('#header').show();
    $('#parcoords-first-page').css("opacity", "0").hide();
    $('#parcoords-after-load').css("opacity", "1").show();

    // Calculate task duration
    let dateformat = Intl.DateTimeFormat('en-GB', {
            year: 'numeric',
            month: 'numeric',
            day: 'numeric',
            hour: 'numeric',
            minute: 'numeric'
        }),
        max_time = data.max_time.replace(/-/g, "/"),
        min_time = data.min_time.replace(/-/g, "/"),
        time_dif = Math.ceil((Date.parse(max_time) - Date.parse(min_time)) / (1000 * 60 * 60 * 24));

    // Write task information
    $('#pc-taskid_value').text(numberWithSpaces(data.jeditaskid));
    $('#pc-jobs_value').text(data.jobs_count);
    $('#pc-finished-failed_value').text(data.finished_count + ' / ' + data.failed_count);
    $('#pc-duration_value').text(time_dif + ' days');
    $('#pc-start_value').text(dateformat.format(Date.parse(min_time)));
    $('#pc-end_value').text(dateformat.format(Date.parse(max_time)));

    // Change the loaded status on the duration page
    if (this.hasOwnProperty('duration')){
        this.duration._storage[data.jeditaskid] = data;
        IDLoaded(data.jeditaskid);
    }

    let linear_interpolation = $('#pc-linear-interpolation').is(':checked');

    // Prepare the configuration object
    this.parcoords = {
        _current_id: data.jeditaskid,
        _data: {
            // statuses: data.statuses,
            scouts: data.scouts,
            failed: data.failed,
            finished: data.finished,
            // closed: data.closed,
            pre_failed: data.pre_failed,
            //sequences: data.sequences
        },
        _options: {
            draw: {                     // Draw options
                mode: "cluster",        // Draw mode: print (no colors) or cluster (with colors)
                parts_visible:          // Visible parts of the diagram
                {
                    cluster_table: true,
                    hint: false,
                    selector: true,
                    table: true,
                    table_colvis: true
                },
                interpolate: (linear_interpolation) ? 'linear' : 'monotone'
            },
            skip: {                     // Feature skip options
                dims: {
                    mode: "show",       // Skip mode: show, hide, none
                    strict_naming: true,
                    values: ['PANDAID', 'DATE_TRUNCATED', 'JOBSTATUS', 'DURATION',
                             'COMPUTINGSITE', 'SITE_WALLTIME_H', 'SITE_ACTUALCORECOUNT_TOTAL', 'SITE_CORECOUNT_TOTAL',
                             'SITE_CPU_UTILIZATION', 'SITE_CPUTIME_H', 'SITE_EFFICIENCY']
                                        // Features to be shown on diagram by default
                },
                table_hide_columns: ['DATE_TRUNCATED', 'IS_SCOUT', 'SEQUENCE', 'START_TS', 'END_TS', 'STATUS_LEVEL']
            },
            worker: {
                enabled: true,
                offscreen: false
            }
        }
    };

    // Prepare the coloring selector
    if (this._selector_div === undefined)
        this._selector_div = d3.select('#color_div')
            .append('select')
                .attr({'class': 'select',
                        'id': 'color_selector'});

    // Set object count on button popups
    $('#scouts_button').data('popup-text', 'Objects count: ' + this.parcoords._data.scouts.data.length);
    $('#finished_button').data('popup-text', 'Objects count: ' + this.parcoords._data.finished.data.length);
    $('#failed_button').data('popup-text', 'Objects count: ' + this.parcoords._data.failed.data.length);
    $('#pre-failed_button').data('popup-text', 'Objects count: ' + this.parcoords._data.pre_failed.data.length);

   // Prepare scatterplot functions
    let scatterplot_data = function (rows, cols) {
            let status_id = cols.indexOf('JOBSTATUS'),
                time = cols.indexOf('MODIFTIME_EXTENDED'),
                site_id = cols.indexOf('COMPUTINGSITE'),
                error_id = cols.indexOf('ERROR_CODE'),
                unique_statuses = rows.map(x => x[status_id]).filter((x,i,arr) => arr.indexOf(x) === i),
                unique_sites = rows.map(x => x[site_id]).filter((x,i,arr) => arr.indexOf(x) === i),
                data = [];

            rows.map(val => data.push([val[0].toString()]));                            // ID
            rows.map((val, i) => data[i].push(val[time]));                              // Modif Time
            rows.map((val, i) => data[i].push(val[status_id],                           // Status
                (Object.keys(window._job_status_colors).includes(val[status_id])) ?     // Color
                    window._job_status_colors[val[status_id]] :
                    ''));
            rows.map((val, i) => data[i].push(val[site_id]));                           // Site
            rows.map((val, i) => data[i].push([val[site_id],
                (val[error_id] === '') ? '-' : val[error_id]]));                        // [Site, Error]

            return {data: data, unique: {statuses: unique_statuses, sites: unique_sites}};
        },
        scatterplot_config = function(data, type, site = ''){
            let //n = 0,
                max_quantity = 20;

            return {
                traces: window._job_status_order.map(x => {
                    let filtered = data.data.filter(y => ((y[2] === x) && (site === '' || y[4] === site))),
                        filtered_rows = [[]];

                    if (filtered.length !== 0)
                        filtered_rows = filtered[0].map((col, i) => filtered.map(row => row[i]));

                    max_quantity = Math.max(filtered_rows[0].length, max_quantity);

                    return {
                        name: x,
                        type: 'scatter',
                        mode: 'markers',
                        visible: window._profiling_statuses_enabled.includes(x) ? '' : 'legendonly',
                        marker: {
                            color: filtered_rows[3],
                            size: 10 //+ n++
                        },
                        hovertemplate:
                            'Panda ID: %{y:,}<br>' +
                            'Status: ' + x + '<br>' +
                            ((site==='') ? 'Site: %{customdata[0]}<br>' : '') +
                            ((x==='failed') ? 'Error: %{customdata[1]}<br>' : '') +
                            'Time: %{x}',
                        hoverlabel: { namelength: 0 },
                        y: filtered_rows[0],
                        x: filtered_rows[1],
                        customdata: filtered_rows[5]
                    }
                }),
                layout: {
                    title: 'Job status profile - ' + type + ((site === '' )? '' : ', site ' + site),
                    xaxis:{
                        tickformat: '%H:%M:%S\n%e %b %Y',
                        title: {
                            text: 'MODIFTIME_EXTENDED'
                        },
                        domain: [0.1, 1]
                    },
                    yaxis: {
                        tickformat: 'd',
                        type: 'category',
                        title: {
                            text: 'Panda ID'
                        }
                    },
                    hovermode:'closest',
                    height: Math.max((max_quantity < 50) ? max_quantity * 20 : 1000, 500)
                }
            }
        };

    // Scatterplot data arrays
    window.scatterplots = {
        'scouts': {},
        'finished': {},
        'failed': {},
        'all': {}
    };


    // Data for all jobs in one object
    let cs_data_all = {data: [], unique: {statuses:[], sites:[]}};

    // Gather all data in cs_data_all and configure individual arrays
    ['scouts', 'finished', 'failed'].forEach(x => {
        let cols = this.parcoords._data[x]['columns'],
            rows = this.parcoords._data[x]['data'],
            _data = scatterplot_data(rows, cols);

        // Data for all statuses
        cs_data_all.data = cs_data_all.data.concat(_data.data);
        cs_data_all.unique.statuses = cs_data_all.unique.statuses.concat(_data.unique.statuses);
        cs_data_all.unique.sites = cs_data_all.unique.sites.concat(_data.unique.sites);

        // Data for this specific status
        window.scatterplots[x].all = scatterplot_config(_data, x);
        window.scatterplots[x]._data = _data;
        _data.unique.sites.forEach((site)=>{
            window.scatterplots[x][site] = scatterplot_config(_data, x, site);
        });
    });

    cs_data_all.unique.statuses = cs_data_all.unique.statuses.filter((x,i,arr) => arr.indexOf(x) === i);
    cs_data_all.unique.sites = cs_data_all.unique.sites.filter((x,i,arr) => arr.indexOf(x) === i);

    window.scatterplots.all.all = scatterplot_config(cs_data_all, 'all');
    cs_data_all.unique.sites.forEach((x)=>{
        window.scatterplots.all[x] = scatterplot_config(cs_data_all, 'all', x);
    });

    // Add 'Filter by' items
    $('#scatter-all-jumpdiv').empty();
    $('#scatter-all-jumpdiv').append('<a class="navigation__link scatter-all active" ' +
        'onclick="SwitchScatterPlot(`all`, `all`, this)">All sites</a>');
    cs_data_all.unique.sites.forEach((x)=>{
        $('#scatter-all-jumpdiv').append('<a class="navigation__link scatter-all" ' +
            'onclick="SwitchScatterPlot(`all`, `' + x + '`, this)">' + x + '</a>');
    });

    // Build the all jobs - all sites plot
    SwitchScatterPlot(`all`, `all`);

    // Disable buttons if there is nothing to display
    if (this.parcoords._data.scouts.data.length === 0) $('#scouts_button').attr("disabled", true);
        else $('#scouts_button').removeAttr("disabled");
    if (this.parcoords._data.finished.data.length === 0) $('#finished_button').attr("disabled", true);
        else $('#finished_button').removeAttr("disabled");
    if (this.parcoords._data.failed.data.length === 0) $('#failed_button').attr("disabled", true);
        else $('#failed_button').removeAttr("disabled");
    if (this.parcoords._data.pre_failed.data.length === 0) $('#pre-failed_button').attr("disabled", true);
        else $('#pre-failed_button').removeAttr("disabled");

    // Draw the diagram
    SwitchDiagram('scouts');
}

// Function to draw scatterplot
// type variable is 'all'/'scouts'/'finished'/'failed'
function SwitchScatterPlot(type, site, navlink){
    let obj = window.scatterplots[type][site],
        tab = (type === 'all') ? 'all' : 'small';

    Plotly.newPlot('status-profile-'+tab, obj.traces, obj.layout);

    if (navlink !== undefined){
        $('.scatter-' + tab).removeClass('active');
        $(navlink).addClass('active');
    }
}


// Function to switch between diagram types: 'scouts', 'finished', etc.
function SwitchDiagram(type, user_approved = false, selector_changed = false){
    // Prepare variable shortcuts
    let pdata = this.parcoords._data,
        options = this.parcoords._options,
        clustering = (type === 'pre_failed') ? 'PRE-FAILED' : 'JOBSTATUS',
        label = '';

    // Clear the search array
    $.fn.dataTable.ext.search = [];

    this._current_type = type;
    if (selector_changed) clustering = selector_changed;

    // Change label and add PRE-FAILED to displayed features list
    if (type === 'scouts') label = 'Scouts';
    else if (type === 'finished') label = 'Finished';
    else if (type === 'failed') label = 'Failed';
    else if (type === 'pre_failed') {
        label = 'Pre-Failed';
        if (!options.skip.dims.values.includes('PRE-FAILED')) options.skip.dims.values.push('PRE-FAILED');
    }
    $('#parcoords-diagram-label').text(label);

    // Make sure ERROR_CODE will be present on all types except Finished
    if (type === 'finished')
        {
            if (options.skip.dims.values.includes('ERROR_CODE'))
                options.skip.dims.values.remove('ERROR_CODE');
        }
    else if (!options.skip.dims.values.includes('ERROR_CODE')) options.skip.dims.values.push('ERROR_CODE');

    let columns = pdata[type]['columns'],
        data = pdata[type]['data'].map(x => x.map(y => (y === 'nan') ? 0 : y)); // nan removerTM

    // Prepare error codes array
    if (data.length !== 0){
        if (!columns.includes('ERROR')) columns.push('ERROR');

        let error_fields = ['EXEERRORCODE', 'EXEERRORDIAG', 'SUPERRORCODE', 'SUPERRORDIAG',
            'DDMERRORCODE', 'DDMERRORDIAG', 'TASKBUFFERERRORCODE', 'TASKBUFFERERRORDIAG',
            'PILOTERRORCODE', 'PILOTERRORDIAG'],
            error_index = error_fields.map((x) => columns.indexOf(x)),

            errors = data.map((x) => x.reduce((con, x, i) => {
                if (i === 1 && !error_index.includes(0)) con = '';
                if (error_index.includes(i) && !['None', '0'].includes(x))
                    con += columns[i].endsWith('CODE') ? x.concat(': ') : x.concat(' <br> ');
                return con;
                })
            );

        data = data.map((x, i) => x.concat([(errors[i] === '') ? 'None' : errors[i].slice(0, -2)]));

        this.parcoords._options.skip.table_hide_columns =
            this.parcoords._options.skip.table_hide_columns.concat(error_fields);
    }

    // Predefined colors for JOBSTATUS field
    let colors = clustering,
        color_scheme = undefined;

    // If clustering by JOBSTATUS - prepare the necessary arrays
    if (clustering === "JOBSTATUS" && pdata[type]['data'].length !== 0)
    {
        colors = data[0].map((col, i) => data.map(row => row[i]))[columns.findIndex(x => x === clustering)];
        let clusters_unique = [...new Set(colors)];
        color_scheme = {order: [], max_count: -1};

        clusters_unique.forEach(x => {
            let count = colors.map(String).count(x);

            color_scheme[x] = {
                count: count,
                color: window._job_status_colors[x]
            };

            if (!color_scheme.hasOwnProperty('min_count')) {
                color_scheme.min_count = 0;
                color_scheme.max_count = count;
            } else {
                color_scheme.min_count = Math.min(count, color_scheme.min_count);
                color_scheme.max_count = Math.max(count, color_scheme.max_count);
            }
        });

        color_scheme.order = clusters_unique.sort((a, b) =>
            color_scheme[b].count - color_scheme[a].count);
    }

    // *** Draw the small scatterplot ***
    if (type !== 'pre_failed'){
        $('#scatter-small-jumpdiv').empty();
        $('#scatter-small-jumpdiv').append('<a id="scatter-small-label" class="navigation__link scatter-small active" ' +
            'onclick="SwitchScatterPlot(`' + type + '`, `all`, this)">All sites</a>');
        window.scatterplots[type]._data.unique.sites.forEach((x)=>{
            $('#scatter-small-jumpdiv').append('<a class="navigation__link scatter-small" ' +
                'onclick="SwitchScatterPlot(`' + type + '`, `' + x + '`, this)">' + x + '</a>');
        });
        SwitchScatterPlot(type, `all`);

        // Add tabs and apply the styling
        $("#parcoords-containter")
            .tabs({
                // Hide unnecessary options divs
                create: () => {
                    $('#scatter-small-options').hide();
                    $('#scatter-all-options').hide();
                },

                // Show them when appropriate tab is selected
                activate: (event, ui) => {
                    switch (ui.newTab.index()) {
                        case 0:
                            $('#parcoords-options').show();
                            $('#scatter-small-options').hide();
                            $('#scatter-all-options').hide();
                            break;
                        case 1:
                            $('#parcoords-options').hide();
                            $('#scatter-small-options').show();
                            $('#scatter-all-options').hide();
                            break;
                        case 2:
                            $('#parcoords-options').hide();
                            $('#scatter-small-options').hide();
                            $('#scatter-all-options').show();
                            break;
                    }
                }
            })
            .removeClass("ui-corner-all ui-widget ui-widget-content");
        $("#parcoords-diagram-container").removeClass("ui-tabs-panel ui-corner-bottom ui-widget-content");
    }
    // *** ScatterPlot finished ***

    // Check for linear interpolation switch
    let linear_interpolation = $('#pc-linear-interpolation').is(':checked'),
        limit_quantity = $('#pc-limit-quantity').is(':checked'),
        duration_id = columns.indexOf('DURATION'),
        color_id = columns.findIndex(x => x === clustering);
    options.draw.interpolate = (linear_interpolation) ? 'linear' : 'monotone';

    // Switch to offscreen when more than 500 lines present
    this.parcoords._options.worker.offscreen = (pdata[type].data.length > 500 );
    // Show a warning if there are more than 5000 lines to draw
    if (pdata[type].data.length > 5000 && !limit_quantity)
        // If this operation is approved, go ahead and draw
        if (!user_approved) {
            $('#parcoords-too-much-data')
                .show()
                .html('The request contains a lot of objects (' + pdata[type].data.length + ') and the diagram ' +
                    'may take a significant time to build. ' +
                    'Are you sure? <button onclick="SwitchDiagram(\'' + type + '\', ' +
                        'true, ' + selector_changed + ')">Yes</button>');
            $('#parcoords-diagram').hide();
            return;
        }

    // If there were too much data and user approved - draw the diagram
    $('#parcoords-too-much-data').hide();
    $('#parcoords-diagram').show();

    // If quantity is limited by the user - sort arrays and extract 400 longest jobs
    if (limit_quantity && data.length > 400){
        data = data.sort((a, b) => b[duration_id] - a[duration_id]).slice(0,400);
        colors = data.map(x => x[color_id]);
        options.worker.offscreen = false;
    }

    // If no diagram was drawn before - init and draw a new one
    if (this.parcoords._diagram === undefined){
        // If nothing to draw - switch to the next type
        if (pdata[type]['data'].length === 0) {
            if (type === 'scouts') SwitchDiagram('finished');
            else if (type === 'finished') SwitchDiagram('failed');
            else if (type === 'failed') SwitchDiagram('pre_failed');
            return;
        }

        this.parcoords._diagram = new ParallelCoordinates("parcoords-diagram",
            columns, data, colors, color_scheme, options);
    }
    else
        // If a diagram is present - update it
        this.parcoords._diagram.updateData("parcoords-diagram", columns, data,
             colors, color_scheme, options);

    // "Color by" selector
    this._selector = $('#color_selector').select2({
            closeOnSelect: true,
            data: columns.map((d) => {
                return {id: d, text: d};
            }),
            width: 230
        })
            .on("change.select2", () => {
                if (this._selector_ready)
                    this.SwitchDiagram(this._current_type, false,
                        this._selector.find(':selected')[0].value);
                else this._selector_ready = true;
            });

    this._selector_ready = false;
    this._selector.val(clustering).trigger('change');
}

function ToggleBoldLines(){
    $('.pc-svg-container .foreground path').css('stroke-width', '5px');
}

// Prepare tooltip popups
function TooltipPrepare() {
    // Prepare the information tooltip
    var tooltipSpan = document.getElementById('tooltip-info');
    $('.icon-info-sign,.info-tooltip')
        .mousemove((e) => {
            let x = e.clientX,
                y = e.clientY;

            tooltipSpan.style.opacity = '1';
            tooltipSpan.style.top = y + 'px';
            tooltipSpan.style.left = (x + 20) + 'px';
            tooltipSpan.textContent = $(e.target).data('popup-text');
        })
        .mouseleave(() => {
            tooltipSpan.style.opacity = '0';
            tooltipSpan.style.top = 9999 + 'px';
            tooltipSpan.style.left = 9999 + 'px';
        });
}

// If an ajax request fails, this function is called
function RequestError(error, id = null){
    $('.pc-loading')
        .html(error.toString())
        .css({'background-image': 'none', 'padding-left': '0px'});

    $('#data-loading')
        .css('background', 'lightpink');

    $('#dates_button').show();

    if (this.hasOwnProperty('duration') && id !== null) {
        IDErrored(id, error.toString());
    }
}