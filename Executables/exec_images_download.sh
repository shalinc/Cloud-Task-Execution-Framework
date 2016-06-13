#!usr/bin/sh
wget --no-verbose -i /home/ubuntu/TaskExecutionFramework/Workloads/image_urls.txt >> /home/ubuntu/TaskExecutionFramework/ResponseLogs/Log$1.txt
x=1; for i in *jpg; do counter=$(printf %d $x); ln -s "$i" /home/ubuntu/TaskExecutionFramework/image"$counter".jpg; x=$(($x+1)); done
ffmpeg -i 'image%d.jpg' -c:v libx264 -preset ultrafast -qp 0 -filter:v "setpts=25.5*PTS" Animoto_video$1.mkv >> /home/ubuntu/Log$1.txt
rm *.jpg*
