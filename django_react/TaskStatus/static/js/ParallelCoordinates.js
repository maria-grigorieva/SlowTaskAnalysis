// Add spaces and a dot to the number
// '1234567.1234 -> 1 234 567.12'
function numberWithSpaces(x) {
    let parts = x.toString().split(".");
    parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, " ");
    return parts.join(".");
}

// RGB color object to hex string
function rgbToHex(color) {
  return "#" + ((1 << 24) + (color.r * 255 << 16) + (color.g * 255 << 8)
      + color.b * 255).toString(16).slice(1);
}

class ParallelCoordinates {
    // ********
    // Constructor
    // 
    // Passes all arguments to updateData(...)
    // ********
    constructor(element_id, dimension_names, data_array, clusters_list, clusters_color_scheme, options = {}) {
        // Save the time for debug purposes
        this._timeStart = Date.now();


        // This function allows to jump to a certain row in a DataTable
        $.fn.dataTable.Api.register('row().show()', function () {
            let page_info = this.table().page.info(),
                // Get row index
                new_row_index = this.index(),
                // Row position
                row_position = this.table().rows()[0].indexOf(new_row_index);
            // Already on right page ?
            if (row_position >= page_info.start && row_position < page_info.end) {
                // Return row object
                return this;
            }
            // Find page number
            let page_to_display = Math.floor(row_position / this.table().page.len());
            // Go to that page
            this.table().page(page_to_display);
            // Return row object
            return this;
        });


        // This is used to manipulate d3 objects
        // e.g., to move a line on a graph to the front
        // https://github.com/wbkd/d3-extended
        d3.selection.prototype.moveToFront = function () {
            return this.each(function () {
                this.parentNode.appendChild(this);
            });
        };
        d3.selection.prototype.moveToBack = function () {
            return this.each(function () {
                let firstChild = this.parentNode.firstChild;
                if (firstChild) {
                    this.parentNode.insertBefore(this, firstChild);
                }
            });
        };


        // Ability to count a number of a certain element in an array
        if (!Array.prototype.hasOwnProperty('count'))
            Object.defineProperties(Array.prototype, {
                count: {
                    value: function (value) {
                        return this.filter(x => x == value).length;
                    }
                }
            });


        // Update data and draw the graph
        if (arguments.length > 0) {
            this.updateData(element_id, dimension_names, data_array, clusters_list, clusters_color_scheme, options);

            if (this._debug)
                console.log("Parallel Coordinates creation finished in %ims", Date.now() - this._timeStart);
        }
    }

    // ********
    // Data loading function
    // 
    // Parameters:
    //  element_id - DOM id where to attach the Parallel Coordinates
    //  feature_names - array with feature names
    //  data_array - array with all data about objects under consideration
    //  clusters_list - array with all clusters in those data
    //  clusters_color_scheme - array with the color scheme
    //  aux_features - auxillary features that are not presented on the graph   -- removed
    //  aux_data_array - auxillaty data                                         -- removed
    //  options - graph options
    //
    // ********
    updateData(element_id, feature_names, data_array, clusters, clusters_color_scheme, options = {}) {
        // Save the time for debug purposes
        this._timeUpdate = Date.now();

        // Store the new values
        this.element_id = element_id;

        // Update arrays
        this._data = {
            _features: feature_names,
            _objects: data_array,
            _color: clusters,
            _color_scheme: clusters_color_scheme
        };
        //this._aux_features = aux_features;
        //this._aux_data = aux_data_array;

        // Debug statistics counters
        this._search_quantity = 0;
        this._search_time = 0;
        this._search_time_min = -1;
        this._search_time_max = -1;

        // If options does not have 'draw' option, make default one
        if (!options.hasOwnProperty('draw') &&
            (typeof this.options === 'undefined' ||
                !this.options.hasOwnProperty('draw'))) {
            options.draw = {
                framework: "d3",    // Possible values: 'd3'. todo: remove 'plotly' back
                mode: "print",       // Possible values: 'print', 'cluster'
                //, first_column_name: "Clusters"    // Custom name for 'clusters' tab in the table
                parts_visible: {
                    table: true,
                    cluster_table: false,
                    hint: true,
                    selector: true,
                    table_colvis: true
                }
            };

            this.options = options;
        } else if (typeof this.options === 'undefined') this.options = options;
        else if (options.hasOwnProperty('draw')) this.options.draw = options.draw;

        // Throw an error if a wrong draw mode selected
        if (!["print", "cluster"].includes(this.options.draw['mode']))
            throw "Wrong mode value! Possible values: 'print', 'cluster', got: '" + value + "'";

        ////// todo: options.draw.parts_visible checks

        // If options does not have 'skip' option, make default one
        // Default is to show 6 first lanes
        if (!options.hasOwnProperty('skip') && !this.options.hasOwnProperty('skip'))
            options.skip = {
                dims: {
                    mode: "show", // Possible values: 'hide', 'show', 'none'
                    values: this._data._features.slice(0,
                        (this._data._features.length >= 5) ? 5 : this._data._features.length),
                    strict_naming: true
                }
            };
        else if (options.hasOwnProperty('skip')) this.options.skip = options.skip;

        this._data.options = this.options;

        // todo: options.skip checks

        // Check debug settings
        if (options.hasOwnProperty('debug')) this._debug = options.debug;
        else if (!this.hasOwnProperty('_debug')) this._debug = false;

        // Initiate the arrays and draw the stuff
        this._prepareGraphAndTables();

        // Show update time when debug enabled
        if (this._debug)
            console.log("Parallel Coordinates updated in %ims (%ims from creation)",
                Date.now() - this._timeUpdate, Date.now() - this._timeStart);

        //console.log(this);
    }

    _prepareGraphAndTables() {
        // A link to this ParCoord object
        var _PCobject = this;

        if (window.Worker) {
            this._worker = this._create_worker();
            //this._worker._PCobject = _PCobject;

            this._call('run', '_prepare_worker', _PCobject._prepare_worker);

            this._call('register', '_prepare_d3', _PCobject._prepare_d3);
            this._call('register', '_prepare_graph', _PCobject._prepare_graph);
            this._call('register', '_update_canvas_size', _PCobject._update_canvas_size);
            this._call('register', '_process_data', _PCobject._process_data);
            this._call('register', '_draw_foreground', _PCobject._draw_foreground);
            this._call('register', '_draw_background', _PCobject._draw_background);
            this._call('register', '_calculate_brushes', _PCobject._calculate_brushes);

            this._worker.onmessage = function (e) {
                let response = e.data;
                //console.log(response);

                if (response.type === 'register') {
                    if (response.name === '_process_data')
                        _PCobject._call('process', '_process_data', '', _PCobject._data);
                }

                if (response.type === 'process') {
                    //console.log('some_process', response.name, response.result);

                    if (response.name === '_process_data') {
                        _PCobject._data = response.result;
                        _PCobject._prepareDOM();
                    }

                    if (response.name === '_prepare_graph')
                    {
                        _PCobject._data._width = response.result._width;
                        _PCobject._data._calculated_height = response.result._calculated_height;
                        _PCobject._data._features_strings_length = response.result._features_strings_length;
                        _PCobject._data._features_strings_params = response.result._features_strings_params;

                        //console.log( _PCobject._data._features_strings_params);

                        // Arrays for x and y data, and brush dragging
                        _PCobject._data._dragging = {};
                    }

                    if (response.name === '_draw_background' || response.name === '_draw_foreground') {
                        if (response.name === '_draw_background') _PCobject._worker._running_bg = false;
                        else _PCobject._worker._running_fg = false;

                        if (_PCobject._worker._bg_needs_redraw || _PCobject._worker._fg_needs_redraw) {
                            _PCobject._redraw(_PCobject._worker._bg_needs_redraw, _PCobject._worker._fg_needs_redraw);

                            _PCobject._worker._bg_needs_redraw = false;
                            _PCobject._worker._fg_needs_redraw = false;
                        }
                    }

                    if (response.name === '_calculate_brushes') {
                        _PCobject._worker._running_brushes = false;

                        if (_PCobject._worker._fg_needs_brushes) {
                            _PCobject._worker._fg_needs_brushes = false;
                            _PCobject._request_brushes(_PCobject._worker._brushes);
                        } else {
                            _PCobject._visible = response.result;
                            _PCobject._datatable.draw();
                            _PCobject._redraw();
                        }
                    }

                }

                if (_PCobject._worker._callback !== undefined) _PCobject._worker._callback.bind(_PCobject)();
            }
        } else {
            this._process_data();
            this._prepareDOM();
        }

        // Clear the whole div if something is there
        $("#" + this.element_id).empty();

        this._loading = d3.select("#" + this.element_id)
            .append('div')
            .attr('class', 'pc-loading')
            .text('Building graph, please wait... ');

        // A selectBox with chosen features
        if (this.options.draw.parts_visible.selector) {
            this._selector_p = d3.select("#" + this.element_id)
                .append('p')
                .text('Select the features displayed on the Parallel Coordinates graph:')
                .style('display', 'none');

            this._selector_p
                .append('select')
                .attr({
                    'class': 'select',
                    'id': 's' + this.element_id
                });
        }

        // Append an SVG to draw lines on
        this._container = d3.select("#" + this.element_id)
            .append('div')
            .attr('class', 'pc-container')
            .style('display', 'none');
        this._svg_container = this._container.append("div")
            .attr('class', 'pc-svg-container');

        this._graph_header = this._svg_container.append("div");
        this._graph_placeholder = this._svg_container.append('div').attr('class', 'pc-graph-placeholder');

        this._canvas_background = this._svg_container.append('canvas');
        this._canvas_foreground = this._svg_container.append('canvas');

        this._offscreen = typeof this._canvas_foreground.node().transferControlToOffscreen === "function";

        if (this._offscreen) {
            this._canvas_foreground_t = this._canvas_foreground.node().transferControlToOffscreen();
            this._canvas_background_t = this._canvas_background.node().transferControlToOffscreen();
        }
        else {
            if (this._d3 === undefined) this._d3 = {};
            this._d3.foreground = this._canvas_foreground.node().getContext('2d');
            this._d3.background = this._canvas_background.node().getContext('2d');
        }
        delete this._canvas_loaded;

        this._graph = this._svg_container.append("svg");

        // Add a tooltip for long names
        this._tooltip = this._svg_container.append("div")
            .attr('class', 'tooltip')
            .style('opacity', 0);

        // A hint on how to use
        if (this.options.draw.parts_visible.hint)
            this._svg_hint = this._svg_container
                .append('p')
                .html('Use the Left Mouse Button to select a curve and the corresponding line in the table <br>' +
                    'Hover over the lines with mouse to see the row in the table');

        // Currently selected line id
        this._selected_line = -1;

        // Add the table below the ParCoords
        if (this.options.draw.parts_visible.table)
            this._container
                .append("div")
                .attr({
                    "id": "t" + this.element_id + "_wrapper-outer",
                    'class': 'pc-table-wrapper'
                });


        if (!window.Worker) {
            this._prepareDOM();
        }
    }

    _prepareDOM() {
        // A link to this ParCoord object
        var _PCobject = this;

        // Options for selectBox
        if (this.options.draw.parts_visible.selector) {
            this._selectBox = $('#s' + this.element_id).select2({
                closeOnSelect: false,
                data: _PCobject._data._features.map((d) => {
                    return {id: d, text: d, selected: _PCobject._data._graph_features.includes(d)};
                }),
                multiple: true,
                width: 'auto'
            })
            // If the list changes - redraw the graph
                .on("change.select2", () => {
                    this._data._graph_features = $('#s' + this.element_id).val();
                    this.options.skip.dims.values = this._data._graph_features;
                    this._call('process', '_prepare_graph', '',
                        {_graph_features: this._data._graph_features},
                        this._createGraph);
                });

            this._selectBox.data('select2').$container.css("display", "block");
        }

        // Draw the graph and the table
        this._createGraph();
        if (this.options.draw.parts_visible.table) this._createTable();

        if (this.options.draw['mode'] === 'cluster' &&
            this.options.draw.parts_visible.cluster_table) {
            this._ci_div = this._container.append('div')
                .attr({
                    "class": 'pc-cluster-table-wrapper',
                    "id": "pc-cluster-" + this.element_id
                });
            // this._createClusterInfo();

            if (this._data._height > 1000)
                $('#pc-cluster-' + this.element_id).insertAfter('#t' + this.element_id + '_wrapper');
        }

        this._loading.style('display', 'none');
        this._container.style('display', '');
        if (this.options.draw.parts_visible.selector) this._selector_p.style('display', '');

        return this;

        // trash bin :)

        /* $("#" + element_id + ".svg")
                .tooltip({
                track: true
                });*/
        // console.log('ids', _ids);

        //console.log(_PCobject);
        //bold[0][i].attr("display", "block");
        //stroke: #0082C866;

        /*_PCobject._datatable.rows().nodes()
            .to$().removeClass('table-selected-line');*/

        //d3.select('#' + this.element_id).style('display', '');
    }

    // Function to draw the graph
    _createGraph(static_height = null) {
        // A link to this ParCoord object
        var _PCobject = this;

        // Clear the graph div if something is there
        if (this._svg !== undefined) this._svg.remove();

        // Initialize a search result with all objects visible and
        // 'visible' data array with lines on foreground (not filtered by a brush)
        this._search_results = this._data._ids;
        this._visible = this._data._ids;

        if (this._graph_popup !== undefined) this._graph_popup.remove();
        this._graph_popup = this._graph_header.append("div")
            .attr('class', 'pc-graph-header')
            .style('display', 'none');

        // Shift the draw space
        this._svg = this._graph.append("g")
            .attr("transform", "translate(" +
                this._data._margin.left + "," + this._data._margin.top + ")");

        // Modify the graph height in case of ordinal values
        if (typeof static_height === 'boolean') this._data._static_height = static_height;

        if (this._data._features_strings_params.overflow_count > 0) {
            let popup_text = (this._data._features_strings_params.overflow_count > 1) ?
                '<b>Info.</b> Multiple features have too many unique values, the graph height was ' +
                'automatically increased to be human readable. ' :
                '<b>Info.</b> Feature "' + this._data._features_strings_params.name + '" has too many unique ' +
                'values, the graph height was automatically increased to be human readable. ',

                popup_link_text = ' <u><i>Click here to return to the default height.</i></u>';

            this._data._height = this._data._calculated_height;

            if (this._data._static_height) {
                this._data._height = this._data._default_height;

                popup_text = (this._data._features_strings_params.overflow_count > 1) ?
                    '<b>Info.</b> Multiple features have too many unique values, the graph height can be ' +
                    'increased to be human readable. ' :
                    '<b>Info.</b> Feature "' + this._data._features_strings_params.name + '" has too many unique ' +
                    'values, the graph height can increased to be human readable. ',

                    popup_link_text = ' <u><i>Click here to increase the height.</i></u>';
            }

            $('#pc-cluster-' + this.element_id).insertAfter('#t' + this.element_id + '_wrapper' +
                ((this._data._height > 1000) ? '' : '-outer'));

            this._graph_popup
                .style('display', '')
                .append('p')
                .attr('class', 'pc-closebtn')
                .on('click', () => {
                    this._graph_popup.style('display', 'none')
                })
                .html('&times;');

            this._graph_popup
                .append('span')
                .attr('class', 'pc-graph-header-text')
                .html(popup_text);
            this._graph_popup
                .append('span')
                .attr('class', 'pc-graph-header-text')
                .on('click', () => this._createGraph(!this._data._static_height))
                .html(popup_link_text);
        } else this._data._height = this._data._default_height;

        this._prepare_d3();

        if (window.Worker && this._offscreen) {
            if (this._canvas_loaded === true)
                this._call('process', '_update_canvas_size',  '',
                    {_height: this._data._height, _width: this._data._width});

            this._call('process', '_prepare_d3', '',
                {_graph_features: this._data._graph_features,
                       _height: this._data._height,
                       _margin: this._data._margin});
        }
        else{
            this._update_canvas_size();
        }

        // Change the SVG size to draw lines on
        this._graph_placeholder.style({
            'width': this._data._width + this._data._margin.left + this._data._margin.right + 'px',
            'height': this._data._height + this._data._margin.top + this._data._margin.bottom + 'px'
        });

        this._graph
            .attr({
                "width": this._data._width + this._data._margin.left + this._data._margin.right,
                "height": this._data._height + this._data._margin.top + this._data._margin.bottom
            });

        this._canvas_foreground
            .style({'margin-left': this._data._margin.left + 'px', "margin-top": this._data._margin.top + 'px'});
        this._canvas_background
            .style({'margin-left': this._data._margin.left + 'px', "margin-top": this._data._margin.top + 'px'});

        this._load_canvas();

        let time = performance.now();

        if(this._offscreen)
            _PCobject._redraw(true);

        else{
            // Grey background lines for context
            this._background = this._svg.append("g")
                .attr("class", "background")
                .selectAll("path")
                .data(this._data._line_data)
                .enter().append("path")
                .attr("d", this._path.bind(this));

             //console.log('bg', performance.now() - time);

            // Foreground lines
            this._foreground = this._svg.append("g")
                .attr("class", "foreground")
                .selectAll("path")
                .data(this._data._line_data)
                .enter().append("path")
                .attr("d", this._path.bind(this))

                // Cluster color scheme is applied to the stroke color
                .attr("stroke", (d, i) => (
                    (this.options.draw['mode'] === "cluster")?
                        this._data._color_scheme[this._data._color[i]].color:
                        "#0082C8")
                    )
                .attr("stroke-opacity", "0.4")

                // When mouse is over the line, make it bold and colorful, move to the front
                // and select a correspoding line in the table below
                .on("mouseover", function (d, i) {
                    if (_PCobject._selected_line !== -1) return;
                    if (_PCobject._isSafari) return;

                    let time = Date.now();

                    $(this).addClass("bold");
                    d3.select(this).moveToFront();

                    if (_PCobject.options.draw.parts_visible.table) {
                        let row = _PCobject._datatable.row((idx, data) => data === _PCobject._parcoordsToTable(i));

                        row.show().draw(false);
                        _PCobject._datatable.rows(row).nodes().to$().addClass('table-selected-line');
                    }
                    // In case of debug enabled
                    // Write time to complete the search, average time, minimum and maximum
                    if (_PCobject._debug)
                    {
                        time = Date.now() - time;
                        _PCobject._search_time += time;
                        _PCobject._search_quantity += 1;

                        if (_PCobject._search_time_min === -1)
                        {
                            _PCobject._search_time_min = time;
                            _PCobject._search_time_max = time;
                        }

                        if (_PCobject._search_time_min > time) _PCobject._search_time_min = time;
                            else if (_PCobject._search_time_max < time) _PCobject._search_time_max = time;

                        console.log("Search completed for %ims, average: %sms [%i; %i].",
                            time, (_PCobject._search_time/_PCobject._search_quantity).toFixed(2),
                            _PCobject._search_time_min, _PCobject._search_time_max);
                    }
                })

                // When mouse is away, clear the effect
                .on("mouseout", function (d, i) {
                    if (_PCobject._selected_line !== -1) return;
                    if (_PCobject._isSafari) return;

                    $(this).removeClass("bold");

                    if (_PCobject.options.draw.parts_visible.table) {
                        let row = _PCobject._datatable.row((idx, data) => data === _PCobject._parcoordsToTable(i));
                        _PCobject._datatable.rows(row).nodes().to$().removeClass('table-selected-line');
                    }
                })

                // Mouse click selects and deselects the line
                .on("click", function (d, i) {
                    if (_PCobject._isSafari) return;

                    if (_PCobject._selected_line === -1) {
                        _PCobject._selected_line = i;

                        $(this).addClass("bold");
                        d3.select(this).moveToFront();

                        if (_PCobject.options.draw.parts_visible.table) {
                            let row = _PCobject._datatable.row((idx, data) => data === _PCobject._parcoordsToTable(i));

                            row.show().draw(false);
                            _PCobject._datatable.rows(row).nodes().to$().addClass('table-selected-line');
                        }
                    }
                    else if (_PCobject._selected_line === i) _PCobject._selected_line = -1;
                });
        }

        //time = performance.now();
        /*

        console.log('fg', performance.now() - time);*/

        // Add a group element for each dimension
        this._g = this._svg.selectAll(".dimension")
            .data(this._data._graph_features)
            .enter().append("g")
            .attr("class", "dimension")
            .attr("transform", function (d) { return "translate(" + _PCobject._d3._x(d) + ")"; })
            .call(d3.behavior.drag()
                .origin(function (d) { return {x: this._d3._x(d)}; }.bind(this))
                .on("dragstart", function (d) {
                    this._data._dragging[d] = this._d3._x(d);
                    if (!this._offscreen) this._background.attr("visibility", "hidden");
                }.bind(this))
                .on("drag", function (d) {
                    this._data._dragging[d] = Math.min(this._data._width, Math.max(0, d3.event.x));
                    this._data._graph_features.sort(function (a, b) {
                        return this._position(a) - this._position(b);
                    }.bind(this));
                    this._d3._x.domain(this._data._graph_features);
                    this._g.attr("transform", function (d) {
                        return "translate(" + this._position(d) + ")";
                    }.bind(this));
                    if (this._offscreen) this._redraw(true);
                        else this._foreground.attr("d", this._path.bind(this));
                }.bind(this))
                .on("dragend", function (d, i) {
                    _PCobject._transition(d3.select(this))
                        .attr("transform", "translate(" + _PCobject._d3._x(d) + ")")
                        .each("end", () => {
                            if (_PCobject._offscreen) {
                                _PCobject._observer.disconnect();
                                delete _PCobject._data._dragging[_PCobject._observer._feature];
                                _PCobject._redraw(false);
                            }
                        });

                    if (!_PCobject._offscreen){
                        delete _PCobject._data._dragging[d];
                        _PCobject._transition(_PCobject._foreground).attr("d", _PCobject._path.bind(_PCobject));

                        _PCobject._background
                            .attr("d", _PCobject._path.bind(_PCobject))
                            .transition()
                            .delay(500)
                            .duration(0)
                            .attr("visibility", null);
                    }
                    else{
                        const config = {attributes: true, childList: false, subtree: false};

                        const callback = function (mutationsList, observer) {
                            // Use traditional 'for loops' for IE 11
                            for (let mutation of mutationsList) {
                                if (mutation.type === 'attributes') {
                                    _PCobject._data._dragging[observer._feature] =
                                        d3.transform(d3.select(mutation.target).attr('transform')).translate[0];
                                    _PCobject._redraw(true);
                                }
                            }
                        };

                        // Create an observer instance linked to the callback function
                        _PCobject._observer = new MutationObserver(callback);
                        _PCobject._observer._feature = d;

                        // Start observing the target node for configured mutations
                        _PCobject._observer.observe(this, config);
                    }
                }));

        // Function to limit the length of the strings
        let format = (x) => {
                if (x instanceof Date) return Intl.DateTimeFormat('en-GB', {
                    year: 'numeric',
                    month: 'numeric',
                    day: 'numeric',
                    hour: 'numeric'
                    //minute: 'numeric'
                }).format(x) + 'h';
                return isNaN(x) ? x : numberWithSpaces(x);
            },

            limit = ((x, width, font) => {
                let sliced = false;

                while (_PCobject._getTextWidth((sliced) ? x + '...' : x, font) > width) {
                    x = x.slice(0, -1);
                    sliced = true;
                }
                return x + ((sliced) ? '...' : '');
            });

        var show_tooltip = (d, width) => {
            if (!limit(d, width, _PCobject._data._font).endsWith('...')) return;

            //on mouse hover show the tooltip
            _PCobject._tooltip.transition()
                .duration(200)
                .style("opacity", .9);
            _PCobject._tooltip.html(d)
                .style("left", (d3.event.layerX + 20) + "px")
                .style("top", (d3.event.layerY + _PCobject._data._margin.top * 2) + "px");
        };

        // Add an axis and titles
        this._g.append("g")
            .attr("class", "axis")
            .each(function (d) {
                d3.select(this).call(_PCobject._d3._axis.scale(_PCobject._d3._y[d]));
            })
            .append("text")
            .attr({
                "y": -9,
                "class": "pc-titles-text"
            })
            .text((text) => limit(text, _PCobject._data._column_width - 15, _PCobject._data._font))
            .on("mouseover", (d) => show_tooltip(d, _PCobject._data._column_width - 15))
            .on("mouseout", () => _PCobject._tooltip.transition().duration(500).style("opacity", 0));

        // Limit the tick length and show a tooltip on mouse hover
        d3.selectAll('.tick')
            .on("mouseover", (obj) => show_tooltip(format(obj), _PCobject._data._column_width - 24))
            .on("mouseout", () => _PCobject._tooltip.transition().duration(500).style("opacity", 0))
            .select('text')
            .text(obj => limit(format(obj), _PCobject._data._column_width - 24, _PCobject._data._font));

        // Add and store a brush for each axis
        this._g.append("g")
            .attr("class", "brush")
            .each(function (d) {
                d3.select(this).call(
                    _PCobject._d3._y[d].brush = d3.svg.brush()
                        .y(_PCobject._d3._y[d])
                        .on("brushstart", _PCobject._brushstart)
                        .on("brush", _PCobject._brush.bind(_PCobject)));
            })
            .selectAll("rect")
            .attr("x", -8)
            .attr("width", 16);
    }

    // Creates a table below the ParallelCoordinates graph
    _createTable() {
        // A link to this ParCoord object
        var _PCobject = this;

        // Clear the table div if something is there
        $('#t' + this.element_id + "_wrapper-outer").empty();

        // Add table to wrapper
        d3.select("#t" + this.element_id + "_wrapper-outer")
            .append("table")
            .attr({
                "id": "t" + this.element_id,
                "class": "table hover"
            });

        // Map headers for the tables
        this._theader = this._data._features.map(row => {
            return {
                title: row,

                // Add spaces and remove too much numbers after the comma
                "render": function (data, type, full) {
                    if (type === 'display' && !isNaN(data))
                        return numberWithSpaces(parseFloat(Number(data).toFixed(2)));

                    return data;
                }
            };
        });

        // Vars for table and its datatable
        this._table = $('#t' + this.element_id);
        this._datatable = this._table.DataTable({
            data: this._data._cells,
            columns: this._theader,

            mark: true,
            dom: 'Blfrtip',
            colReorder: true,
            stateSave: true,
            buttons: ((this.options.draw.parts_visible.table_colvis) ? ['colvis'] : []).concat(['copy', 'csv']),
            "search": {"regex": true},

            columnDefs: [
                {width: 20, targets: 0}
            ],
            fixedColumns: true,

            // Make colors lighter for readability
            "rowCallback": (row, data) => {
                if (this.options.draw['mode'] === "cluster")
                    $(row).children().css('background', data[data.length - 1] + "33");

                $(row).children().css('white-space', 'nowrap');
            },

            // Redraw lines on ParCoords when table is ready
            "fnDrawCallback": () => {
                _PCobject._on_table_ready(_PCobject);
            }
        });

        this._fix_css_in_table('t' + this.element_id);

        // Add bold effect to lines when a line is hovered over in the table
        /*$(this._datatable.table().body())
            .on("mouseover", 'tr', function (d, i) {
                if (_PCobject._selected_line !== -1) return;

                let line = _PCobject._foreground[0][_PCobject._tableToParcoords(
                    _PCobject._datatable.row(this).data())];
                $(line).addClass("bold");
                d3.select(line).moveToFront();

                $(_PCobject._datatable.rows().nodes()).removeClass('table-selected-line');
                $(_PCobject._datatable.row(this).nodes()).addClass('table-selected-line');
            })
            .on("mouseout", 'tr', function (d) {
                if (_PCobject._selected_line !== -1) return;

                $(_PCobject._datatable.rows().nodes()).removeClass('table-selected-line');

                $(_PCobject._foreground[0][
                    _PCobject._tableToParcoords(_PCobject._datatable.row(this).data())
                ]).removeClass("bold");
            })

            // If the line is clicked, make it 'selected'. Remove this status on one more click.
            .on("click", 'tr', function (d, i) {
                if (_PCobject._selected_line === -1) {
                    _PCobject._selected_line = _PCobject._tableToParcoords(_PCobject._datatable.row(this).data());

                    let line = _PCobject._foreground[0][_PCobject._selected_line];
                    $(line).addClass("bold");
                    d3.select(line).moveToFront();

                    _PCobject._datatable.rows(this).nodes().to$().addClass('table-selected-line');
                }
                else if (_PCobject._selected_line === _PCobject._tableToParcoords(
                    _PCobject._datatable.row(this).data())) {
                        let line = _PCobject._foreground[0][_PCobject._selected_line];
                        $(line).removeClass("bold");

                        _PCobject._selected_line = -1;
                        _PCobject._datatable.rows(this).nodes().to$().removeClass('table-selected-line');
                    }
            });*/

        // Add footer elements
        this._table.append(
            $('<tfoot/>').append($('#t' + this.element_id + ' thead tr').clone())
        );

        // Add inputs to those elements
        $('#t' + this.element_id + ' tfoot th').each(function (i, x) {
            $(this).html('<input type="text" placeholder="Search" id="t' +
                _PCobject.element_id + 'Input' + i + '"/>');
        });

        // Apply the search
        this._datatable.columns().every(function (i, x) {
            $('#t' + _PCobject.element_id + 'Input' + i).on('keyup change', function () {
                _PCobject._datatable
                    .columns(i)
                    .search(this.value, true)
                    .draw();
            });
        });

        // Callback for _search_results filling
        $.fn.dataTable.ext.search.push(
            function (settings, data, dataIndex, rowData, counter) {
                if (settings.sTableId !== "t" + _PCobject.element_id) return true;

                if (counter === 0) _PCobject._search_results = [];

                if (_PCobject._visible.some(x => Object.values(x).every(y => data.includes(y)))) {
                    _PCobject._search_results.push(data);
                    return true;
                }
                return false;
            }
        );
    }

    // Create cluster info buttons (which call the table creation)
    _createClusterInfo() {
        // Add a div to hold a label and buttons
        this._ci_buttons_div = this._ci_div
            .append('div')
            .attr('class', 'ci-buttons-wrapper');

        // Add 'Choose Cluster' text to it
        this._ci_buttons_div
            .append('label')
            .text("Choose Cluster");

        // Add a div for the table
        this._ci_table_div = this._ci_div.append('div');

        //Add a div to hold the buttons after the label
        this._ci_buttons = this._ci_buttons_div
            .append('div')
            .attr({
                'class': 'ci-button-group',
                'id': 'ci_buttons_' + this.element_id
            });

        let scheme = this._data._color_scheme,
            scale = d3.scale.sqrt()
                .domain([scheme.min_count, scheme.max_count])
                .range([100, 0]);

        // Add corresponding buttons to every color
        this._ci_buttons
            .selectAll("a")
            .data(scheme.order)
            .enter().append('a')
            .attr({
                'class': 'ci-button',
                'title': id => "Cluster " + id + ".\nElement count: " + scheme[id].count + "."
            })
            .style('background', id => 'linear-gradient(90deg, ' + scheme[id].color +
                ' ' + (100 - scale(scheme[id].count)) + '%, white ' + (101 - scale(scheme[id].count)) + '%)')
            .text(id => id)
            .on("click", id => {
                d3.event.preventDefault();

                // Apply the activated class
                this._ci_buttons_div.attr('class', 'ci-buttons-wrapper ci-buttons-active');

                // Clean all children
                this._ci_table_div
                //.style('border', "5px dashed " + this._color_scheme[id].color + "33")
                    .attr('class', 'ci-table pc-table-wrapper')
                    .html('');

                // Add the 'selected' decoration
                this._ci_buttons_div.selectAll('*').classed('ci-selected', false);
                d3.select(d3.event.target).classed('ci-selected', true);

                // Add 'Cluster # statistics' text
                this._ci_table_div
                    .append('h3')
                    .text("Cluster " + d3.event.target.innerText + " statistics");

                // Print the stats
                this._createClusterStatsTable();
            });
    }

    // Creates a table with cluster info
    // The function must be called from onClick, as it uses the d3.event.target
    _createClusterStatsTable() {
        // A link to this ParCoord object
        var _PCobject = this;

        // Make the header array
        this._ci_header = ['', "Min", "Mean", "Max", "Median", "Deviation"].map((x, i) => {
            return {
                title: x,
                className: (i === 0) ? 'firstCol' : '',

                // Add spaces and remove too much numbers after the comma
                "render": function (data, type, full) {
                    if (type === 'display' && !isNaN(data))
                        return numberWithSpaces(parseFloat(Number(data).toFixed(2)));

                    return data;
                }
            }
        });

        // Prepare data and values arrays for calculations
        this._ci_cluster_data = this._data._objects.filter((x, i) =>
            String(this._data._color[i]) === d3.event.target.innerText);
        this._ci_cluster_values = this._ci_cluster_data[0].map((col, i) =>
            this._ci_cluster_data.map(row => row[i]));

        // Prepare table cells
        this._ci_cells = this._data._features.map((x, i) =>
            (this._isNumbers(x)) ?
                [
                    x,
                    d3.min(this._ci_cluster_data, row => (row[i] === null) ? 0 : row[i]),
                    d3.mean(this._ci_cluster_data, row => (row[i] === null) ? 0 : row[i]),
                    d3.max(this._ci_cluster_data, row => (row[i] === null) ? 0 : row[i]),
                    d3.median(this._ci_cluster_data, row => (row[i] === null) ? 0 : row[i]),
                    (this._ci_cluster_data.length > 1) ? d3.deviation(this._ci_cluster_data, row =>
                        (row[i] === null) ? 0 : row[i]) : '-'
                ] : [x + ((this._isStrings(x)) ? ' <i>(click to expand)</i>' : ''), '-', '-', '-', '-', '-']);

        // Calculate stats for string values
        this._ci_string_stats = this._data._features_strings.map((name) => [name,
            [...new Set(
                this._ci_cluster_values[
                    this._data._features.findIndex((x) => x === name)
                    ])
            ].map(x => [x,
                this._ci_cluster_values[this._data._features.findIndex((x) => x === name)].count(x)])]);

        // Add 'Number of elements: N' text
        this._ci_table_div
            .append('h5')
            .text('Number of elements: ' + this._ci_cluster_data.length);

        // Create the table
        this._ci_table_div
            .append('table')
            .attr('id', 'ci_table_' + this.element_id);

        // Add the data to the table
        let table = $('#ci_table_' + this.element_id).DataTable({
            data: this._ci_cells,
            columns: this._ci_header,
            mark: true,
            dom: 'ABlfrtip',
            colReorder: true,
            buttons: ['copy', 'csv'],
            "search": {"regex": true}
        });

        // Add line getting darker on mouse hover
        $(table.table().body())
            .on("mouseover", 'tr', function (d, i) {
                $(table.rows().nodes()).removeClass('table-selected-line');
                $(table.row(this).nodes()).addClass('table-selected-line');
            })
            .on("mouseout", 'tr', function (d) {
                $(table.rows().nodes()).removeClass('table-selected-line');
            })
            // Add event listener for opening and closing details
            .on('click', 'td.firstCol', function () {
                if (!this.innerText.endsWith(' (click to expand)') || _PCobject._ci_string_stats === []) return;

                let feature = this.innerText.replace(' (click to expand)', ''),
                    id = _PCobject._data._features_strings.indexOf(feature),
                    table_id = 'ci-' + _PCobject.element_id + '-' + id,
                    tr = $(this).closest('tr'),
                    row = table.row(tr),
                    text = '<table id="' + table_id + '" class="ci_aux_table" style="width:min-content">';

                _PCobject._ci_string_stats[id][1].forEach(x => {
                    text += '<tr><td>' + x[0] + '</td><td> ' + x[1] + '</td></tr>'
                });

                text += '</table>';

                if (row.child.isShown()) {
                    // This row is already open - close it
                    row.child.hide();
                    tr.removeClass('shown');
                } else {
                    // Open this row
                    row.child(text).show();
                    tr.addClass('shown');

                    let table = $('#' + table_id).DataTable({
                        columns: [
                            {title: feature},
                            {title: "Count"}
                        ],
                        dom: 't',
                        order: [[1, "desc"]]
                    });

                    $(table.table().body())
                        .on("mouseover", 'tr', function () {
                            $(table.rows().nodes()).removeClass('table-selected-line');
                            $(table.row(this).nodes()).addClass('table-selected-line');
                        })
                        .on("mouseout", 'tr', function () {
                            $(table.rows().nodes()).removeClass('table-selected-line');
                        });
                }
            });

        // Fix the css
        this._fix_css_in_table('ci_table_' + this.element_id);
    }

    // Functions to perform id transformation
    _tableToParcoords(object) {
        return this._data._cells.findIndex(x => object.every((y, i) => y === x[i]));
    }

    _parcoordsToTable(index) {
        return this._data._cells[index];
    }

    _isNumbers(featureName) {
        return this._data._features_numbers.includes(featureName);
    }

    _isDate(featureName) {
        return this._data._features_dates.includes(featureName);
    }

    _isStrings(featureName) {
        return this._data._features_strings.includes(featureName);
    }

    // Callback to change the lines visibility after 'draw()' completed
    _on_table_ready(object) {
        if (!object._offscreen)
            object._foreground.style("display", function (d, j) {
                return object._search_results
                        .some(x => x
                            .every((y, i) =>
                                y === object._data._ids[j][i]))
                    ? null : "none";
            });
    }

    // Bug fixes related to css
    _fix_css_in_table(id) {
        d3.select('#' + id + '_wrapper')
            .insert("div", ".dataTables_filter + *")
            .attr('class', 'pc-table-contents')
            .node()
            .appendChild(document.getElementById(id));
    }

    // ***
    // Additional functions

    // Functions for lines and brushes
    _position(d) {
        let v = this._data._dragging[d];
        return v == null ? this._d3._x(d) : v;
    }

    _transition(g) {
        return g.transition().duration(500);
    }

    _brushstart() {
        d3.event.sourceEvent.stopPropagation();
    }

    /**
     * Uses canvas.measureText to compute and return the width of the given text of given font in pixels.
     *
     * @param {String} text The text to be rendered.
     * @param {String} font The css font descriptor that text is to be rendered with (e.g. "bold 14px verdana").
     *
     * @see https://stackoverflow.com/questions/118241/calculate-text-width-with-javascript/21015393#21015393
     */
    _getTextWidth(text, font) {
        // re-use canvas object for better performance
        let canvas = this._getTextWidth.canvas || (this._getTextWidth.canvas = document.createElement("canvas"));
        let context = canvas.getContext("2d");
        context.font = font;
        let metrics = context.measureText(text);
        return metrics.width;
    }

    // ***
    // Connecting with the worker

    _create_worker(){
        // Browser detection
        // Safari 3.0+ "[object HTMLElementConstructor]"
        let isSafari = /constructor/i.test(window.HTMLElement) || (function (p) {
            return p.toString() === "[object SafariRemoteNotification]"; })(!window['safari'] ||
            (typeof safari !== 'undefined' && safari.pushNotification)),

            runner = function _worker_runner(){
                self._isSafari = safari_test;

                self.onmessage = function(e) {
                    //console.log('Message received from main script', this, e.data);
                    //var workerResult = 'Result: ' + (e.data[0] * e.data[1]);

                    if (!this.hasOwnProperty('_functions')) this._functions = {};
                    let data = e.data;
                    this._data = data;

                    if (!self._isSafari) data.function = 'function ' + data.function;
                    //console.log(data.type, data.name, data.data);

                    if(data.type === 'register')
                        this._functions[data.name] = eval('( ' + data.function + ')');

                    if(data.type === 'process')
                        (this._functions[data.name].bind(this))();

                    if(data.type === 'run')
                        (eval('( ' + data.function + ')').bind(this))();

                    //console.log('Posting message back to main script');
                    self.postMessage({'type': data.type, 'name': data.name, 'result': data.data});
                };
            },

            workerBlob = new Blob(
            [runner.toString()
                .replace(/^function .+\{?|\}$/g, '')
                .replace('safari_test', isSafari)],
            { type:'text/javascript' }
            ),

            workerBlobUrl = URL.createObjectURL(workerBlob);

        this._isSafari = isSafari;
        return new Worker(workerBlobUrl);
    }

    // Call a function in the worker
    _call(type, name, func, data = '', callback = undefined){
        if (window.Worker){
            this._worker.postMessage({
                    type: type,
                    name: name,
                    function: func.toString(),
                    data: data
            });
            this._worker._callback = callback;
        }
    }

    _load_canvas() {
        if (this._offscreen && this._canvas_loaded === undefined) {
            this._worker.postMessage({
                type: 'run',
                name: '_load_canvas_worker',
                function: this._load_canvas_worker.toString(),
                foreground: this._canvas_foreground_t,
                background: this._canvas_background_t,
                data: {
                    _height: this._data._height,
                    _width: this._data._width
                }
            }, [this._canvas_foreground_t, this._canvas_background_t]);
            this._canvas_loaded = true;
        }
    }

    // Handles a brush event, toggling the display of foreground lines
    _brush() {
        //let time = performance.now();
        var _PCobject = this;

        let brushes = {
            actives: this._data._graph_features.filter(function (p) {
                return !_PCobject._d3._y[p].brush.empty();
            })
        };

        brushes.extents = brushes.actives.map(function (p) {
            return _PCobject._d3._y[p].brush.extent();
        });

        this._request_brushes(brushes);
    }

    // Returns the path for a given data point
    _path(d) {
        return this._d3._line(
            this._data._graph_features.map(
                function (p) { return [this._position(p), this._d3._y[p](d[p])]; },
                this
            )
        );
    }


    _redraw(redraw_bg = false, redraw_fg = true) {
        //console.log('redraw', redraw_bg, redraw_fg);

        if (this._worker._running_bg || this._worker._running_fg) {
            this._worker._fg_needs_redraw = redraw_fg || this._worker._fg_needs_redraw;
            this._worker._bg_needs_redraw = redraw_bg || this._worker._bg_needs_redraw;
        } else {
            if (!this._offscreen) {
                return;
                //if (redraw_fg) this._draw_foreground();
                //if (redraw_bg) this._draw_background();
            }

            if (redraw_fg) {
                this._worker._running_fg = true;
                this._call('process', '_draw_foreground', '',
                    {_dragging: this._data._dragging,
                        _graph_features: this._data._graph_features,
                        _search_results: this._search_results});
            }

            if (redraw_bg) {
                this._worker._running_bg = true;
                this._call('process', '_draw_background', '',
                    {_dragging: this._data._dragging,
                        _graph_features: this._data._graph_features});
            }
        }
    }

    _request_brushes(brushes) {
        //console.log('redraw', redraw_bg, redraw_fg);

        if (this._worker._running_brushes) {
            this._worker._fg_needs_brushes = true;
            this._worker._brushes = brushes;
        } else {
            this._worker._running_brushes = true;
            this._worker.postMessage({
                type: 'process', name: '_calculate_brushes',
                _brushes: brushes
            });
        }
    }

    // *******
    // Worker functions

    // Initialize worker
    _prepare_worker() {
        // Ability to count a number of a certain element in an array
        if (!Array.prototype.hasOwnProperty('count'))
            Object.defineProperties(Array.prototype, {
                count: {
                    value: function (value) {
                        return this.filter(x => x == value).length;
                    }
                }
            });

        importScripts('https://d3js.org/d3.v3.min.js');
        importScripts('https://d3js.org/d3-shape.v1.min.js');
        if (this._d3 === undefined) this._d3 = {};
    }

    // Load canvases into worker
    _load_canvas_worker() {
        this._d3.foreground = this._data.foreground.getContext('2d');
        this._d3.background = this._data.background.getContext('2d');
        self._offscreen = true;
        this._functions._update_canvas_size.bind(this)();
    }

    // Update canvas size
    _update_canvas_size() {
        let data = (this._data.hasOwnProperty('data')) ? this._data.data : this._data;
        this._d3._line = (self._offscreen) ?
            d3.line().curve(d3.curveMonotoneX) :
            d3.svg.line().interpolate("monotone");

        this._d3.foreground.canvas.height = data._height;
        this._d3.foreground.canvas.width = data._width;

        this._d3.background.canvas.height = data._height;
        this._d3.background.canvas.width = data._width;
    }

    // Calculate the arrays
    _process_data() {
        //console.log('process_data call', this);

        if (this._d3 === undefined) this._d3 = {};

        let data = (this._data.hasOwnProperty('data')) ? this._data.data : this._data;
        if (this.hasOwnProperty('_copy')) delete this._copy;

        let has_empty = data.options.skip['dims'].strict_naming ||
            data.options.skip['dims'].values.some(x => x === "");

        // Construct the list with dimentions on graph
        data._graph_features = data._features.filter(elem => {
            let skip = data.options.skip;

            if (!('dims' in skip)) return true;
            if (skip['dims'].mode === 'none') return true;
            if (skip['dims'].mode === 'show') {
                if (has_empty && elem === "") return true;
                else if (elem === "") return false;

                if (skip['dims'].strict_naming) {
                    if (skip['dims'].values.some(x => x === elem)) return true;
                } else if (skip['dims'].values.some(x => (x !== "") && (x.includes(elem) || elem.includes(x))))
                    return true;
            }

            return skip['dims'].mode === 'hide' &&
                !skip['dims'].values.some(x => (x.includes(elem) || elem.includes(x)));
        });

        // Remove all line breaks
        //data._objects = data._objects.map((row) => row.map(obj => obj.replace(/\r?\n|\r/, "")));

        // Reference array with all values as strings
        data._ids = data._objects.map((row) => row.map(String));

        // Transposed data for future work
        data._values = data._objects[0].map((col, i) => data._objects.map(row => row[i]));

        // Arrays with numbers-only and string data parts
        data._features_numbers = data._features.filter((name, i) => data._values[i].every(x => !isNaN(x)));
        data._features_dates = data._features.filter((name, i) =>
            !data._features_numbers.includes(name) && data._values[i].every(x => !isNaN(Date.parse(x))));
        data._features_strings = data._features.filter((name) =>
            !data._features_numbers.includes(name) && !data._features_dates.includes(name));

        // Make scales for each feature
        data._date_values = {};
        data._features_dates.forEach(dim => {
            data._date_values[dim] =
                data._values[data._features.indexOf(dim)].map(Date.parse);
        });

        // Coloring modes if clustering enabled
        if (data.options.draw.mode === "cluster") {
            let clusters = data._color,
                color_scheme = data._color_scheme;

            // Clusters array can be null. In this case clustering is done automatically by the 2nd column.
            if (typeof clusters === 'undefined' ||
                clusters === null ||
                clusters === [])
                clusters = data._features[1];

            // Next, if we got a string - consider it as a clustering column.
            if (typeof clusters === 'string')
            // In case we got no scematics - generate a new one.
                if (typeof color_scheme === 'undefined' ||
                    color_scheme === null ||
                    color_scheme === []) {
                    data._color = data._values[data._features.findIndex(x => x === clusters)];

                    let clusters_unique = [...new Set(data._color)];

                    data._color_scheme = {order: [], min_count: -1, max_count: -1};

                    clusters_unique.forEach(x => {
                        let count = data._color.map(String).count(x);

                        data._color_scheme[x] = {
                            count: count
                        };

                        if (!data._color_scheme.hasOwnProperty('min_count')) {
                            data._color_scheme.min_count = count;
                            data._color_scheme.max_count = count;
                        } else {
                            data._color_scheme.min_count = Math.min(count, data._color_scheme.min_count);
                            data._color_scheme.max_count = Math.max(count, data._color_scheme.max_count);
                        }
                    });

                    data._color_scheme.order = clusters_unique.sort((a, b) =>
                        data._color_scheme[b].count - data._color_scheme[a].count);

                    let colorscale = d3.scale.category20();
                    data._color_scheme.order.forEach((x, i) =>
                        data._color_scheme[x].color = colorscale(i));
                }
        }

        // Future datatable cells (w/ color if present)
        data._cells = (data.options.draw['mode'] === "cluster") ?
            data._ids.map((x, i) => x.concat([data._color_scheme[data._color[i]].color])) :
            data._ids;

        // ----- Some graph preparations -----
        // Sizes of the graph
        data._margin = {top: 30, right: 10, bottom: 10, left: 45};
        data._column_width = 120;
        data._default_height = 500 - data._margin.top - data._margin.bottom;
        data._height = data._default_height;

        // Arrays for x and y data, and brush dragging
        data._dragging = {};

        // Font desctiption
        data._font = '\'Oswald script=all rev=1\' 10px sans-serif';

        (this._functions._prepare_graph.bind(this))();
        (this._functions._prepare_d3.bind(this))();

        if (this._data.hasOwnProperty('data'))
            this._copy = data;

        //console.log('process_finished', this._data, this);
    }

    _prepare_graph(){
        let data = (this._data.hasOwnProperty('data')) ? this._data.data : this._data,
            storage = (this.hasOwnProperty('_copy')) ? this._copy : data;

        data._width = (data._graph_features.length > 5 ?
                storage._column_width * data._graph_features.length :
                storage._column_width * 6) -
                storage._margin.left - storage._margin.right;



        data._calculated_height = storage._default_height;
        data._features_strings_length = [];
        data._features_strings_params = {overflow_count: 0};

        data._graph_features.forEach(dim => {
            if (storage._features_strings.includes(dim)) {
                let count = [...new Set(storage._values[storage._features.indexOf(dim)])].length;

                data._features_strings_length.push({
                    id: dim,
                    count: count
                });

                if (count * 18 > storage._default_height) {
                    data._calculated_height = Math.max(data._calculated_height, count * 17 -
                        storage._margin.top - storage._margin.bottom);

                    data._features_strings_params.name = dim;
                    data._features_strings_params.overflow_count += 1;
                }
            }
        });
    }

    // Prepare d3
    _prepare_d3() {
        let data = (this._data.hasOwnProperty('data')) ? this._data.data : this._data,
            storage = (this.hasOwnProperty('_copy')) ? this._copy : data;
        if (this._d3 === undefined) this._d3 = {};

        if (this.hasOwnProperty('_copy')) {
            storage._graph_features = data._graph_features;
            storage._height = data._height;
            storage._margin = data._margin;

            storage._width = (storage._graph_features.length > 5 ?
                storage._column_width * storage._graph_features.length :
                storage._column_width * 6) -
                storage._margin.left - storage._margin.right;

            delete this._data.data;
        }

        this._d3._x = d3.scale.ordinal().rangePoints([0, storage._width], 1).domain(storage._graph_features);

        this._d3._y = {};
        this._d3._ranges = {};
        // Make scales for each feature
        storage._graph_features.forEach(dim => {
            if (storage._features_numbers.includes(dim)) {
                let min = Math.min(...storage._values[storage._features.indexOf(dim)]),
                    max = Math.max(...storage._values[storage._features.indexOf(dim)]),
                    domain = [min, max];

                // if (0 <= min && min <= 100 && 0 <= max && max <= 100) domain = [0, 100];
                // if (0 <= min && min <= 10 && 0 <= max && max <= 10) domain = [0, 10];
                // if (0 <= min && min <= 1 && 0 <= max && max <= 1) domain = [0, 1];

                this._d3._y[dim] = d3.scale.linear().domain(domain).nice().range([storage._height, 0]);
            } else if (storage._features_dates.includes(dim)) {
                this._d3._y[dim] = d3.time.scale()
                    .domain([Math.min(...storage._date_values[dim]),
                        Math.max(...storage._date_values[dim])])
                    .nice()
                    .range([storage._height, 0]);
            } else {
                this._d3._y[dim] = d3.scale.ordinal()
                    .domain(storage._values[storage._features.indexOf(dim)])
                    .rangePoints([storage._height, 0]);
                this._d3._ranges[dim] = this._d3._y[dim].domain().map(this._d3._y[dim]);
            }
        });

        // Line and axis parameters, arrays with lines (gray and colored)
        this._d3._line = (self._offscreen) ?
            d3.line().curve(d3.curveMonotoneX) :
            d3.svg.line().interpolate("monotone");
        this._d3._axis = d3.svg.axis().orient("left");

        // Array to make brushes
        storage._line_data = storage._objects.map((x, i) =>{
            let tmp = {};
            storage._graph_features.forEach((f) => tmp[f] = (storage._features_dates.includes(f) ?
                    storage._date_values[f][i] : x[storage._features.indexOf(f)]));
            return tmp;
        });
    }

    // Draw foreground canvas
    _draw_foreground() {
        //console.log('draw call', this);

        let time = Date.now();

        let data = (this._data.hasOwnProperty('data')) ? this._data.data : this._data,
            storage = (this.hasOwnProperty('_copy')) ? this._copy : data,
            search_results = (this.hasOwnProperty('_search_results')) ? this._search_results : data._search_results;

        this._d3.foreground.clearRect(0, 0, storage._width + storage._margin.left + storage._margin.right,
            storage._height + storage._margin.top + storage._margin.bottom);

         if (this.hasOwnProperty('_copy')){
             storage._graph_features = data._graph_features;
             delete this._data.data;
         }

        // console.log('draw', storage._graph_features, data._dragging);
        this._d3._x.domain(storage._graph_features);

        let pos = (d) => {
                let v = data._dragging[d];
                return v == null ? this._d3._x(d) : v;
            },

            draw = (d, i, ctx) => {
                //console.log('draw', Object.values(d));
                if (!search_results.some(x => d.every(y => x.includes(y)))) return;
                let y = -1;

                ctx.strokeStyle = (storage.options.draw['mode'] === "cluster") ?
                    storage._color_scheme[storage._color[i]].color :
                    "#0082C866";

                ctx.lineWidth = 1;

                ctx.beginPath();

                this._d3._line.context(ctx)(
                    storage._graph_features.map(
                        function (p) { return [pos(p), this._d3._y[p](
                            (storage._features_dates.includes(p)) ?
                                storage._date_values[p][i] :
                                d[storage._features.indexOf(p)])]; },
                        this
                    )
                );

                ctx.stroke();
            };

        storage._ids.forEach((d, i) => draw(d, i, this._d3.foreground));
        //console.log('draw fg ',Date.now() - time);
        //console.log('draw finished');//, this);

    }

    // Draw background canvas
    _draw_background() {
        //console.log('draw bg call', this);

        let time = Date.now();
        let data = (this._data.hasOwnProperty('data')) ? this._data.data : this._data,
            storage = (this.hasOwnProperty('_copy')) ? this._copy : data;

        this._d3.background.clearRect(0, 0, storage._width + storage._margin.left + storage._margin.right,
            storage._height + storage._margin.top + storage._margin.bottom);

        if (this.hasOwnProperty('_copy')){
             storage._graph_features = data._graph_features;
             delete this._data.data;
         }

        this._d3._x.domain(storage._graph_features);

        let pos = (d) => {
                let v = data._dragging[d];
                return v == null ? this._d3._x(d) : v;
            },

            draw = (d, i, ctx) => {
                ctx.strokeStyle = "#ddd";
                ctx.lineWidth = 1;

                ctx.beginPath();

                this._d3._line.context(ctx)(
                    storage._graph_features.map(
                        function (p) { return [pos(p), this._d3._y[p](d[p])]; },
                        this
                    )
                );

                ctx.stroke();
            };

        storage._line_data.forEach((d, i) => draw(d, i, this._d3.background));

        //console.log('draw bg ',Date.now() - time);
        //console.log('draw finished');//, this);
    }

    // Calculate brush filter results
    _calculate_brushes() {
        //console.log('_calculate_brushes', this);

        let data = (this._data.hasOwnProperty('type')) ? this._copy : this._data,
            brushes = this._data._brushes,
            visible = [];

        //console.log('_calculate_brushes input', this);
        if (brushes.actives.length === 0) visible = data._ids;
        else data._line_data.forEach(function (d, j) {
            let isVisible = brushes.actives.every(function (p, i) {
                let value = null;

                if (data._features_strings.includes(p))
                    value = this._d3._ranges[p][this._d3._y[p].domain().findIndex(x => x === d[p])];
                else value = d[p];
                //console.log('_calculate_brushes input', this);
                return brushes.extents[i][0] <= value && value <= brushes.extents[i][1];
            });

            if (isVisible) visible.push(data._ids[j]);
        });

        //console.log('_calculate_brushes result', visible);
        if (this._data.hasOwnProperty('type')) this._data.data = visible;
        else this._visible = visible;
    }
}