gnuplot << \EOF
set terminal postscript
set output "| ps2pdf - ReadLatency.pdf"
set xlabel "Time"
set ylabel "Read Latency"
set xdata time
set timefmt "%H:%M:%S"
set format x "%H:%M:%S"
set title "Read Latency Time Series"
set style line 1 lt rgb "#A00000" lw 2 pt 1
set style line 2 lt rgb "#00A000" lw 2 pt 6
set style line 3 lt rgb "#5060D0" lw 2 pt 2
set   autoscale                        # scale axes automatically
unset log                              # remove any log-scaling
unset label                            # remove any previous labels
set xtic auto                          # set xtics automatically
set ytic auto                          # set ytics automatically
plot    "Plot.txt" using 1:2 title 'Read Latency' with linespoints ls 1
EOF
