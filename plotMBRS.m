% run on Matlab R2016b
% load points and create a scatter plot
rangedPoints = load('rpUpdated');
figure();
scatter(rangedPoints(:,1),rangedPoints(:,2));
%load and plot mbrs as a green overlay over points with alpha 0.2
%Patch takes coordinates in the form lower_left, upper_left, upper_right,
%lower_right, in 2 separate arrays
%our points were recorded as [minx, miny, maxx, maxy], thus the
%interleaving
r = load('mbrsupd');
hold on;
for i = 1:size(r,1)
    x = [r(i,1); r(i,1); r(i,3); r(i,3)];
    y = [r(i,2); r(i,4); r(i,4); r(i,2)];
    a = patch(x,y,'g');
    a.FaceAlpha = 0.2;
end

xlabel('x coordinate');
ylabel('y coordinate');
axis equal;
title('Part 1: Points and MBRs with equal scaling per dimension');