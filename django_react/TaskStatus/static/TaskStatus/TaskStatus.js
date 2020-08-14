// Function to switch between 'Task Analysis' and 'Parallel Coordinates' tabs
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

// Send a request to server when the user has chosen the dates on the Task Analysis tab
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
    $.ajax({
        url: '/ajax/request_db',
        data: { 'type': 'get-slowest-tasks', 'start-time': start_time, 'end-time' : end_time},
        dataType: 'json',
        timeout: 0,
        success: (data) => GotDurationData(data),
        error: (error) => RequestError('There was an error: ' + error.statusText)
    });

    // Request boxplot info
    $.ajax({
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
            '  <img class="top-link-img" data-type="link" src="/static/images/external-link.png" title="Open in new tab"> ' +
            //'class="duration-img duration-load" style="width:10px;height:10px;">' +
            '</a>'; },
        load_btn = '<img data-type="load" src="/static/images/view.png" ' +
            'title="Prepare the diagram" class="duration-img duration-load">',
        loading_icon = '<img src="/static/ParallelCoordinates/loading.gif" data-type="loading" ' +
            'title="Data is loading" class="duration-img hidden img-unclickable">',
        ready_icon = '<img src="/static/images/checkmark.png" data-type="ready" ' +
            'title="Data is ready" class="duration-img hidden">',
        error_icon = '<img src="/static/images/delete.png" data-type="ready" ' +
            'title="There was an error. Please try to load the data again." ' +
            'class="duration-img hidden img-unclickable">',
        forward_btn = '<img src="/static/images/line-chart.png" data-type="run" ' +
            'title="Draw Parallel Coordinates diagram" class="duration-img hidden">';

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

    if(this._duration_table !== undefined){
        this._duration_table.clear().draw();
        this._duration_table.rows.add(this.duration._cells);
        this._duration_table.columns.adjust().draw();
    }
    else
        this._duration_table = $('#slowest-tasks-list').DataTable({
            data: this.duration._cells,
            columns: this.duration._header,
            mark: true,
            bAutoWidth: false,
            dom: 'ABlfrtip',
            buttons: ['copy', 'csv'],
            "order": [[ 1, "desc" ]],
            "searching": false
        });
    $('#slowest-tasks-list')
        .addClass("hover");

    $('#slowest-tasks-list tbody').on( 'click', 'img', function () {
        let jeditaskid = window._duration_table.row($(this).parents('tr')).data()[0].toString();

        if (this.dataset.type === 'run') RunParCoords(jeditaskid);
        else if (['loading', 'link'].includes(this.dataset.type)) return;
        else PreLoadIDinfo(jeditaskid);
    });
}

// Function to set the start and finish date on the Task Analysis page
function SetDates(type){
    let date = new Date();
    document.getElementById('duration-finish').value = date.toISOString().split('T')[0];

    if (type === 'last-month') date.setMonth(date.getMonth() - 1);
    else date.setDate(date.getDate() - 1);

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
            'done': [],
            'finished': [],
            'aborted': [],
            'exhausted': [],
            'failed': [],
            'obsolete': [],
            'broken': [],
            'ready': []
        },
        traces = [],
        layout = {
            yaxis: { title: 'Execution time, days' },
            title: 'Box Plot: Distribution of tasks execution time by statuses'
        },
        config = {responsive: true};

    data.data.forEach((x) => plot_data[x[1]].push(x[0]));

    ['done', 'finished', 'aborted', 'exhausted', 'failed', 'obsolete', 'broken', 'ready']
        .forEach((x) =>
            traces.push({
                y: plot_data[x],
                type: 'box',
                name: x,
                boxpoints: 'Outliers'
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
            BuildParCoords(this.duration._storage[id]);
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
        error: (error) => RequestError('There was an error: ' + error.statusText, id)
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
function IDErrored(id, error = 'There was an error. Please try to load the data again.'){
    let node = this._duration_table.cell(IDtoTable(id), ':last-child').node();
    $($(node).children()[0]).show();
    $($(node).children()[1]).hide();
    $($(node).children()[2]).hide();
    $($(node).children()[3]).show().attr('title', error);
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
        success: (data) => BuildParCoords(data),
        error: (error) => RequestError('There was an error: ' + error.statusText, value)
      });

    if (this.hasOwnProperty('duration')) IDLoading(value);
}

// A function to initialize ParCoords diagram
function BuildParCoords(data) {
    if (data.hasOwnProperty('error')){
        RequestError(data.error, (data.hasOwnProperty('jeditaskid')) ? data.jeditaskid : null);
        return;
    }

    $('#header').show();
    $('#parcoords-first-page').css("opacity", "0").hide();
    $('#parcoords-after-load').css("opacity", "1").show();

    let dateformat = Intl.DateTimeFormat('en-GB', {
            year: 'numeric',
            month: 'numeric',
            day: 'numeric',
            hour: 'numeric',
            minute: 'numeric'
        }),
        time_dif = Math.ceil((Date.parse(data.max_time) - Date.parse(data.min_time)) / (1000 * 60 * 60 * 24));

    $('#pc-taskid_value').text(numberWithSpaces(data.jeditaskid));
    $('#pc-jobs_value').text(data.jobs_count);
    $('#pc-finished-failed_value').text(data.finished_count + ' / ' + data.failed_count);
    $('#pc-duration_value').text(time_dif + ' days');
    $('#pc-start_value').text(dateformat.format(Date.parse(data.min_time)));
    $('#pc-end_value').text(dateformat.format(Date.parse(data.max_time)));

    if (this.hasOwnProperty('duration')){
        this.duration._storage[data.jeditaskid] = data;
        IDLoaded(data.jeditaskid);
    }

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
                }
            },
            skip: {                     // Feature skip options
                dims: {
                    mode: "show",       // Skip mode: show, hide, none
                    strict_naming: true,
                    values: ['PANDAID','DATE_TRUNCATED','JOBSTATUS','DURATION',
                        'COMPUTINGSITE','SITE_EFFICIENCY', 'ERROR']   // Features to be shown on diagram by default
                },
                table_hide_columns: ['DATE_TRUNCATED', 'IS_SCOUT', 'SEQUENCE', 'START_TS', 'END_TS', 'STATUS_LEVEL', 'ERROR_CODE']
            },
            worker: {
                enabled: true,
                offscreen: false
            }
        }
    };

    $('#scouts_button').prop('title', 'Objects count: ' + this.parcoords._data.scouts.data.length);
    $('#finished_button').prop('title', 'Objects count: ' + this.parcoords._data.finished.data.length);
    $('#failed_button').prop('title', 'Objects count: ' + this.parcoords._data.failed.data.length);
    $('#pre-failed_button').prop('title', 'Objects count: ' + this.parcoords._data.pre_failed.data.length);

    if (this.parcoords._data.scouts.data.length === 0) $('#scouts_button').attr("disabled", true);
        else $('#scouts_button').removeAttr("disabled");
    if (this.parcoords._data.finished.data.length === 0) $('#finished_button').attr("disabled", true);
        else $('#finished_button').removeAttr("disabled");
    if (this.parcoords._data.failed.data.length === 0) $('#failed_button').attr("disabled", true);
        else $('#failed_button').removeAttr("disabled");
    if (this.parcoords._data.pre_failed.data.length === 0) $('#pre-failed_button').attr("disabled", true);
        else $('#pre-failed_button').removeAttr("disabled");

    $.fn.dataTable.ext.search = [];

    SwitchDiagram('scouts');
}

// Function to switch between diagram types: 'scouts', 'finished', etc.
function SwitchDiagram(type, user_approved = false){
    let pdata = this.parcoords._data,
        options = this.parcoords._options,
        clustering = (type === 'pre_failed') ? 'PRE-FAILED' : 'JOBSTATUS',
        label = '';

    if (type === 'scouts') label = 'Scouts';
    else if (type === 'finished') label = 'Finished';
    else if (type === 'failed') label = 'Failed';
    else if (type === 'pre_failed') {
        label = 'Pre-Failed';
        if (!options.skip.dims.values.includes('PRE-FAILED')) options.skip.dims.values.push('PRE-FAILED');
    }

    $('#parcoords-diagram-label').text(label);

    this.parcoords._options.worker.offscreen = (pdata[type].data.length > 500);
    if (pdata[type].data.length > 5000)
        if (!user_approved) {
            $('#parcoords-too-much-data')
                .show()
                .html('The request contains a lot of objects (' + pdata[type].data.length + ') and the diagram ' +
                    'may take a significant time to build. ' +
                    'Are you sure? <button onclick="SwitchDiagram(\'' + type + '\', true)">Yes</button>');
            $('#parcoords-diagram').hide();
            return;
        }

    $('#parcoords-too-much-data').hide();
    $('#parcoords-diagram').show();

    let columns = pdata[type]['columns'],
        data = pdata[type]['data'];

    if (data.length !== 0){
        if (!columns.includes('ERROR')) columns.push('ERROR');

        let error_fields = ['EXEERRORCODE', 'EXEERRORDIAG', 'SUPERRORCODE', 'SUPERRORDIAG',
            'DDMERRORCODE', 'DDMERRORDIAG', 'TASKBUFFERERRORCODE', 'TASKBUFFERERRORDIAG',
            'PILOTERRORCODE', 'PILOTERRORDIAG'],
            error_index = error_fields.map((x) => columns.indexOf(x)),

            errors = data.map((x) => x.reduce((con, x, i) => {
                if (i === 1 && !error_index.includes(0)) con = '';
                if (error_index.includes(i) && !['None', '0'].includes(x))
                    con += columns[i].endsWith('CODE') ? x.concat(': ') : x.concat(' | ');
                return con;
                })
            );

        data = data.map((x, i) => x.concat([(errors[i] === '') ? 'None' : errors[i].slice(0, -2)]));

        this.parcoords._options.skip.table_hide_columns =
            this.parcoords._options.skip.table_hide_columns.concat(error_fields);
    }

    if (this.parcoords._diagram === undefined){
        if (pdata[type]['data'].length === 0) {
            if (type === 'scouts') SwitchDiagram('finished');
            else if (type === 'finished') SwitchDiagram('failed');
            else if (type === 'failed') SwitchDiagram('pre_failed');
            return;
        }
        this.parcoords._diagram = new ParallelCoordinates("parcoords-diagram",
            columns, data, clustering, null, options);
    }
    else
        this.parcoords._diagram.updateData("parcoords-diagram", columns, data,
            clustering, null, options);
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