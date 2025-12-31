FROM ruby:3.2

RUN apt-get update && \
    apt-get install -y postgresql-client && \
    rm -rf /var/lib/apt/lists/*

ENV APP_HOME /cl2-tenant-clone
WORKDIR $APP_HOME

COPY Gemfile* $APP_HOME/
RUN bundle install

COPY . $APP_HOME

RUN mkdir -p tmp/dumps

CMD ["bash"]
