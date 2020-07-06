function ChangeTab(tab){
    $(tab).parent().children().removeClass('header-button-selected');
    $(tab).addClass('header-button-selected');

    if (tab.id === 'header-taskanalysis'){
        $('#parcoords-contents').css({"left": "100vw","opacity": "0"});
        $('#duration-contents').css( {"left": "0", "opacity": "1"});
    }
    else
    {
        $('#parcoords-contents').css({"left": "0", "opacity": "1"});
        $('#duration-contents').css( {"left": "-100vw", "opacity": "0"});
    }
}

function DatesChosen(){
    $('#dates_loading').show();
    $('#dates_button').hide();

    setTimeout(() => GotDurationData({}), 1000); // ajax placeholder
}

function GotDurationData(data){
    $('#duration-description').css('opacity', '0');
    $('.duration-block-right').css('opacity', '1');
    $('#dates_loading').hide();

    let load_btn = '<img data-type="load" src="/static/images/load-from-cloud.png" ' +
            'title="Load the data" class="duration-img duration-load">',
        loading_icon = '<img src="/static/ParallelCoordinates/loading.gif" data-type="loading" ' +
            'title="The data is loading" class="duration-img img-hidden img-unclickable">',
        ready_icon = '<img src="/static/images/checkmark.png" data-type="ready" ' +
            'title="The data is loaded. Click to load the data again." class="duration-img img-hidden">',
        error_icon = '<img src="/static/images/delete.png" data-type="ready" ' +
            'title="There was an error. Please try to load the data again." class="duration-img img-hidden img-unclickable">',
        forward_btn = '<img src="/static/images/forward.png" data-type="run" ' +
            'title="Build ParCoords diagrams" class="duration-img img-hidden">';

    this.duration = {
        _storage: {},
        _header: ['taskid', "time", ''].map((x, i) => {
            return {
                title: x,
                className: (i === 0) ? 'firstCol' : '',

                // Add spaces and remove too much numbers after the comma
                "render": function (data, type, full) {
                    if (type === 'display' && !isNaN(data))
                        return numberWithSpaces(parseFloat(Number(data).toFixed(2)));

                    if (data==='btns') return load_btn + loading_icon + ready_icon + error_icon + forward_btn;

                    return data;
                }
            }
        }),
        _cells: [
            [21250253, '32', 'btns'],
            [21250254, '48', 'btns'],
            [21250255, '25', 'btns'],
            [21250256, '21', 'btns']
        ]
    };

    this.duration._table = $('#slowest-tasks-list').DataTable({
        data: this.duration._cells,
        columns: this.duration._header,
        mark: true,
        dom: 'ABlfrtip',
        buttons: ['copy', 'csv'],
        "searching": false
    });

    $('#slowest-tasks-list tbody').on( 'click', 'img', function () {
        let data = window.duration._table.row($(this).parents('tr')).data();
        if (this.dataset.type==='run') RunParCoords(data[0]);
        else if (this.dataset.type==='loading') return;
        else PreLoadIDinfo(data[0]);
    });
}

function RunParCoords(id){
    if (!this.duration._storage.hasOwnProperty(id)) PreLoadIDinfo(data[0]);
    else {
        $('#header-parallelcoordinates').click();

        if (!this.hasOwnProperty('parcoords') || this.parcoords._current_id !== id)
            BuildParCoords(this.duration._storage[id]);
    }
}

function PreLoadIDinfo(id){
    IDLoading(id);

    $.ajax({
        url: '/ajax/request_db',
        data: { 'jeditaskid': id },
        dataType: 'json',
        success: (data) => StoreData(data),
        error: (error) => RequestError('There was an error: ' + error.statusText, id)
    });
}

function StoreData(data){
    if (data.hasOwnProperty('error')){
        RequestError(data.error, (data.hasOwnProperty('jeditaskid')) ? data.jeditaskid : null);
        return;
    }

    this.duration._storage[data.jeditaskid] = data;
    IDLoaded(data.jeditaskid);
}

function IDtoTable(id){
    return this.duration._cells.findIndex(x => x[0] === id);
}

function IDLoading(id){
    let node = this.duration._table.cell(IDtoTable(id), 2).node();
    $($(node).children()[0]).hide();
    $($(node).children()[1]).show();
    $($(node).children()[2]).hide();
    $($(node).children()[3]).hide();
    $($(node).children()[4]).hide();
}

function IDLoaded(id){
    let node = this.duration._table.cell(IDtoTable(id), 2).node();
    $($(node).children()[0]).hide();
    $($(node).children()[1]).hide();
    $($(node).children()[2]).show();
    $($(node).children()[3]).hide();
    $($(node).children()[4]).show();
}

function IDErrored(id, error = 'There was an error. Please try to load the data again.'){
    let node = this.duration._table.cell(IDtoTable(id), 2).node();
    $($(node).children()[0]).show();
    $($(node).children()[1]).hide();
    $($(node).children()[2]).hide();
    $($(node).children()[3]).show().attr('title', error);
    $($(node).children()[4]).hide();
}

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
        data: { 'jeditaskid': value },
        dataType: 'json',
        success: (data) => BuildParCoords(data),
        error: (error) => RequestError('There was an error: ' + error.statusText, value)
      });

    if (this.hasOwnProperty('duration')) IDLoading(value);
}

function BuildParCoords(data) {
    if (data.hasOwnProperty('error')){
        RequestError(data.error, (data.hasOwnProperty('jeditaskid')) ? data.jeditaskid : null);
        return;
    }

    $('#header').show();
    $('#parcoords-first-page').hide();
    $('#parcoords-after-load').show();
    $('#taskid_header').text('TaskID = ' + data.jeditaskid + ' (failed jobs statuses exploration)');

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
            sequences: data.sequences
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
                    mode: "show",           // Skip mode: show, hide, none
                    strict_naming: true,
                    values: ['PANDAID','DATE_TRUNCATED','JOBSTATUS','DURATION',
                        'COMPUTINGSITE','SITE_EFFICIENCY', 'ERROR_CODE']   // Features to be shown on diagram by default
                }
            },
            worker: {
                enabled: true,
                offscreen: false
            }
        }
    };

    $.fn.dataTable.ext.search = [];

    let pdata = this.parcoords._data,
        options = this.parcoords._options;

    this.parcoords.scouts_pc = new ParallelCoordinates("scouts_pc",
        pdata.scouts['columns'], pdata.scouts['data'], 'JOBSTATUS', null, options);

    this.parcoords.failed_pc = new ParallelCoordinates("failed_pc",
        pdata.failed['columns'], pdata.failed['data'], 'JOBSTATUS', null, options);

    this.parcoords.finished_pc = new ParallelCoordinates("finished_pc",
        pdata.finished['columns'], pdata.finished['data'], 'JOBSTATUS', null, options);

    /*
    this.parcoords.closed_pc = new ParallelCoordinates("closed_pc",
         pdata.closed['columns'], pdata.closed['data'], 'JOBSTATUS', null, options);

     this.parcoords.all_failed_pc = new ParallelCoordinates("all_failed_pc",
         pdata.statuses['columns'], pdata.statuses['data'], 'JOBSTATUS', null, options);
     */

    this.parcoords.pre_failed_pc = new ParallelCoordinates("pre_failed_pc",
        pdata.pre_failed['columns'], pdata.pre_failed['data'], 'PRE-FAILED', null, options);

}

function RequestError(error, id = null){
    $('.pc-loading')
        .html(error.toString())
        .css({'background-image': 'none', 'padding-left': '0px'});

    $('#data-loading')
        .css('background', 'lightpink');

    if (this.hasOwnProperty('duration') && id !== null) {
        IDErrored(id, error.toString());
    }
}