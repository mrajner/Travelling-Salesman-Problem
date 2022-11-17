#!/usr/bin/env ruby

require 'sequel'

db = Sequel.mysql2("miasta")
l = 10

miasta = db[:miasta]
  .where(ifmiasto: true).or(przyszlemiasto: true)
  .order(:uniqname)
  .select(:uniqname)

odleglosci = db[:sph]

wybrane = odleglosci
  .where(fromcity: miasta.limit(l))
  .where(tocity: miasta.limit(l))

table = db[:miasta]
  .with(:wybrane, wybrane)
  .from(:wybrane)
  .left_join(db[:miasta], {:uniqname => Sequel.qualify(:wybrane, :fromcity)}, :table_alias => :f)
  .left_join(db[:miasta], {:uniqname => Sequel.qualify(:wybrane, :tocity)}, :table_alias => :t)
  .select_map([:fromcity, Sequel[:f][:lat], Sequel[:f][:lon], :tocity, Sequel[:t][:lat], Sequel[:t][:lon], :distance])
  .map{|e|
    e[0] = e[0].delete(" \t\r\n")
    e[3] = e[3].delete(" \t\r\n")
    e.join(' ')
  }.join("\n") + "\n"

File.write("table.dat", table)

puts "Brute force will require #{Math.gamma(l).to_i/2} operations"

