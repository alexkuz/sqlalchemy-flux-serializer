from flask import request, url_for


class FlaskMetadataProvider(object):
    @staticmethod
    def get_metadata(next_model_id):
        if not next_model_id:
            return None

        url_args = {}
        url_args.update(request.view_args)
        url_args.update(request.args)
        url_args['after'] = next_model_id

        next_url = url_for(request.endpoint, **url_args)

        return {
            'next': next_url
        }

    @staticmethod
    def get_after_id():
        return request.args.get('after')        
