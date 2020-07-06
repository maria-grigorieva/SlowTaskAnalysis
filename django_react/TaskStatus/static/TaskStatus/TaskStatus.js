function ChangeTab(tab){
    $(tab).parent().children().removeClass('header-button-selected');
    $(tab).addClass('header-button-selected');

    $('#parcoords-contents').css("left", (tab.id === 'header-taskanalysis')? "100vw":"0");
}

function GetIDinfo(id){
    const value = parseInt(id);

    if (isNaN(value)) {
        alert('Please enter an integer value of the task ID');
        return;
    }

    $('#taskid-selector').hide();
    $('.pc-loading').show();
    document.title = value + ' -  Parallel Coordinates';

    $.ajax({
        url: '/ajax/request_db',
        data: { 'jeditaskid': value },
        dataType: 'json',
        success: (data) => BuildParCoords(data),
        error: (error) => RequestError('There was an error: ' + error.statusText)
      });
}

function BuildParCoords(data) {
    if (data.hasOwnProperty('error')){
        RequestError(data.error);
        return;
    }

    $('#header').show();
    $('#parcoords-first-page').hide();
    $('#parcoords-after-load').show();
    $('#taskid_header').text('TaskID = ' + data.jeditaskid + ' (failed jobs statuses exploration)');

    this.parcoords = {
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

function RequestError(error){
    $('.pc-loading')
        .html(error.toString())
        .css({'background-image': 'none', 'padding-left': '0px'});

    $('#data-loading')
        .css('background', 'lightpink');
}